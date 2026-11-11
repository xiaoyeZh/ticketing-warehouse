"""真实 Spark + PostgreSQL：日报口径、事务发布、只读报表与流水线重跑。"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import os
import subprocess
import sys
from unittest.mock import patch
from uuid import UUID

import psycopg

from warehouse_fixture import WarehouseFixture
from ticketing.payment import PaymentService
from warehouse.aggregate import aggregate, verify_aggregate
from warehouse.aggregate_runs import AggregateStore, ZERO
from warehouse.errors import WarehouseError
from warehouse.pipeline import PipelineStore, run_pipeline
from warehouse.publication import publish
from warehouse.report import report
from warehouse.spark import create_session
from warehouse.transform import transform


class PublicationTests(WarehouseFixture):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = create_session('ticketing-daily-publication-tests')
        cls.addClassCleanup(cls.spark.stop)

    def model(self):
        source = self.run_extract()
        return transform(source['source_batch_id'], data_dir=self.root, writer_factory=self.writer, spark=self.spark)

    def daily(self, model=None):
        return aggregate((model or self.model())['run_id'], data_dir=self.root, writer_factory=self.writer, spark=self.spark)

    def publish(self, daily, **kwargs):
        return publish(daily['aggregate_run_id'], writer_factory=self.writer, **kwargs)

    def report(self, **kwargs):
        return report(reporter_factory=lambda: self.as_role('reporter'), **kwargs)

    def pipeline(self, **kwargs):
        return run_pipeline(data_dir=self.root, reader_factory=self.reader, writer_factory=self.writer, spark=self.spark, **kwargs)

    def test_daily_grain_counts_money_and_snapshot_names(self):
        self.paid_order()
        model = self.model()
        with self.admin() as conn:
            conn.execute("UPDATE activity SET title='后来改的名称'")
        daily = self.daily(model)
        self.assertEqual(daily['row_count'], 1)
        self.assertEqual(daily['measured_totals'], {'sold_ticket_count':2,'paid_order_count':1,'paid_amount':'220.00'})
        self.assertTrue(verify_aggregate(daily['aggregate_run_id'],writer_factory=self.writer)['verified'])
        self.publish(daily)
        result = self.report()
        self.assertEqual(result['totals'], daily['measured_totals'])
        self.assertEqual(result['rows'][0]['activity_title'], '事务测试活动')
        self.assertEqual(result['rows'][0]['paid_amount'], '220.00')

    def test_refund_resale_keeps_original_sale(self):
        order = self.paid_order()
        service = PaymentService(self.runtime)
        service.refund(self.users[0], order['order_id'], reason='重售')
        with self.admin() as conn:
            conn.execute('UPDATE session_seat SET price=150 WHERE session_id=%s', (self.sessions[0],))
        resale = self.reserve(user=1)
        service.pay(self.users[1], resale['order_id'], Decimal('300'))
        daily = self.daily()
        self.publish(daily)
        self.assertEqual(self.report()['totals'], {'sold_ticket_count':4,'paid_order_count':2,'paid_amount':'520.00'})

    def test_multiple_sessions_have_separate_daily_groups(self):
        self.paid_order()
        order = self.service().reserve(self.users[1],self.sessions[1],self.inventory[1])
        PaymentService(self.runtime).pay(self.users[1],order['order_id'],Decimal('220'))
        daily = self.daily()
        self.publish(daily)
        result = self.report()
        self.assertEqual(len(result['rows']),2)
        self.assertEqual({r['session_id'] for r in result['rows']},set(self.sessions))
        self.assertEqual(result['totals'],{'sold_ticket_count':4,'paid_order_count':2,'paid_amount':'440.00'})

    def test_midnight_zero_amount_and_date_filters(self):
        with self.admin() as conn:
            conn.execute('UPDATE session_seat SET price=0 WHERE session_id=%s', (self.sessions[0],))
        for seat, paid_at in zip(self.inventory[0], (datetime(2026,11,3,15,59,tzinfo=timezone.utc),
                                                     datetime(2026,11,3,16,0,tzinfo=timezone.utc))):
            order = self.reserve(seats=[seat])
            PaymentService(self.runtime).pay(self.users[0], order['order_id'], Decimal('0'))
            with self.admin() as conn:
                conn.execute('UPDATE booking_order SET paid_at=%s WHERE order_id=%s', (paid_at,order['order_id']))
        daily = self.daily()
        self.assertEqual(daily['row_count'], 2)
        self.publish(daily)
        result = self.report(start_date=date(2026,11,3),end_date=date(2026,11,4),session_id=self.sessions[0])
        self.assertEqual(result['totals'], {'sold_ticket_count':1,'paid_order_count':1,'paid_amount':'0.00'})
        self.assertEqual(result['rows'][0]['paid_date'],'2026-11-03')
        self.assertEqual(self.report(session_id=self.sessions[1])['totals'],ZERO)
        with self.assertRaisesRegex(WarehouseError,'INVALID_REPORT_FILTER'):
            self.report(start_date=date(2026,11,4),end_date=date(2026,11,3))

    def test_empty_publication_is_distinct_from_no_publication(self):
        with self.assertRaisesRegex(WarehouseError,'REPORT_NOT_PUBLISHED'):
            self.report()
        daily = self.daily()
        self.assertEqual(daily['row_count'],0)
        self.publish(daily)
        self.assertEqual(self.report()['rows'],[])
        self.assertEqual(self.report()['totals'],ZERO)

    def test_repeat_aggregate_does_not_double_count_and_publish_is_idempotent(self):
        self.paid_order()
        model = self.model()
        first, second = self.daily(model), self.daily(model)
        self.assertNotEqual(first['aggregate_run_id'],second['aggregate_run_id'])
        self.assertEqual(first['data_sha256'],second['data_sha256'])
        initial = self.publish(first)
        self.assertEqual(initial,self.publish(first,expected_generation=0))
        self.publish(second,expected_generation=1)
        self.assertEqual(self.report()['totals'],first['measured_totals'])
        self.assertEqual(self.value('SELECT count(*) AS value FROM wh_meta.publication_event'),2)

    def test_load_failure_rolls_back_all_daily_rows(self):
        self.paid_order()
        model = self.model()
        with patch('warehouse.aggregate_runs.check_loaded',side_effect=OSError('injected after insert')):
            with self.assertRaises(WarehouseError) as caught:
                self.daily(model)
        run_id = UUID(caught.exception.identifiers['aggregate_run_id'])
        self.assertEqual(AggregateStore(self.writer).get(run_id)['status'],'FAILED')
        self.assertEqual(self.value('SELECT count(*) AS value FROM dws.session_sales_day'),0)
        with self.assertRaisesRegex(WarehouseError,'AGGREGATE_NOT_SUCCEEDED'):
            publish(run_id,writer_factory=self.writer)

    def test_lost_load_reply_resolves_committed_success(self):
        self.paid_order()
        original = AggregateStore.load
        def lose_reply(store,*args):
            original(store,*args)
            raise OSError('lost acknowledgement')
        with patch.object(AggregateStore,'load',lose_reply):
            result = self.daily()
        self.assertTrue(result['verified'])
        self.assertEqual(result['status'],'SUCCEEDED')

    def test_failed_publication_preserves_old_pointer_and_event_history(self):
        self.paid_order()
        model = self.model()
        first, second = self.daily(model), self.daily(model)
        self.publish(first)
        with patch('warehouse.publication._complete_pipeline',side_effect=OSError('injected after pointer change')):
            with self.assertRaisesRegex(WarehouseError,'PUBLICATION_FAILED'):
                self.publish(second)
        self.assertEqual(self.report()['publication']['aggregate_run_id'],first['aggregate_run_id'])
        self.assertEqual(self.value('SELECT count(*) AS value FROM wh_meta.publication_event'),1)

    def test_lost_publish_reply_resolves_event_without_second_publication(self):
        daily = self.daily()
        from warehouse.publication import _result
        count = 0
        def lose_reply(*args):
            nonlocal count
            count += 1
            if count == 1:
                raise OSError('lost reply after commit')
            return _result(*args)
        with patch('warehouse.publication._result',side_effect=lose_reply):
            published = self.publish(daily)
        self.assertEqual(published['generation'],1)
        self.assertEqual(self.value('SELECT count(*) AS value FROM wh_meta.publication_event'),1)

    def test_corrupt_dws_is_rejected_before_publication(self):
        self.paid_order()
        daily = self.daily()
        with self.writer() as conn:
            conn.execute('UPDATE dws.session_sales_day SET paid_amount=999 WHERE aggregate_run_id=%s',
                         (UUID(daily['aggregate_run_id']),))
        with self.assertRaisesRegex(WarehouseError,'AGGREGATE_DATA_MISMATCH'):
            self.publish(daily)
        self.assertEqual(self.value('SELECT generation AS value FROM wh_meta.publication'),0)

    def test_concurrent_publication_has_one_winner(self):
        self.paid_order()
        model = self.model()
        batches = [self.daily(model),self.daily(model)]
        def attempt(batch):
            try:
                return self.publish(batch,expected_generation=0)['status']
            except WarehouseError as error:
                return error.code
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(attempt,batches))
        self.assertCountEqual(outcomes,['PUBLISHED','PUBLICATION_CONFLICT'])
        self.assertEqual(self.report()['publication']['generation'],1)

    def test_older_snapshot_requires_explicit_override(self):
        self.paid_order()
        older = self.daily()
        newer = self.daily()
        self.publish(newer)
        with self.assertRaisesRegex(WarehouseError,'OLDER_SNAPSHOT_REQUIRES_OVERRIDE'):
            self.publish(older)
        self.publish(older,expected_generation=1,allow_older=True)
        self.assertEqual(self.report()['publication']['aggregate_run_id'],older['aggregate_run_id'])

    def test_report_uses_one_snapshot_during_publication(self):
        self.paid_order()
        first = self.daily()
        self.publish(first)
        # 新快照具有不同的可见业务字段。
        with self.admin() as conn:
            conn.execute("UPDATE activity SET title='新名称'")
        second = self.daily()
        test = self
        class Connection:
            def __enter__(self):
                self.conn = test.as_role('reporter')
                return self
            def __exit__(self,*args):
                return self.conn.__exit__(*args)
            def transaction(self):
                return self.conn.transaction()
            def execute(self,statement,*args):
                cursor = self.conn.execute(statement,*args)
                if statement == 'SELECT * FROM ads.session_sales_publication':
                    test.publish(second)
                return cursor
        snapshot = report(reporter_factory=Connection)
        self.assertEqual(snapshot['publication']['aggregate_run_id'],first['aggregate_run_id'])
        self.assertEqual(snapshot['rows'][0]['activity_title'],'事务测试活动')
        self.assertEqual(self.report()['rows'][0]['activity_title'],'新名称')

    def test_reporter_permissions_and_cli_without_writer_credentials(self):
        self.paid_order()
        self.publish(self.daily())
        for statement in ('SELECT * FROM public.booking_order','SELECT * FROM wh_meta.publication',
                          'SELECT * FROM dws.session_sales_day'):
            with self.as_role('reporter') as conn:
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    conn.execute(statement)
        with self.as_role('reporter') as conn:
            rights = conn.execute('''SELECT has_table_privilege(current_user,'ads.session_sales_day','SELECT')
                AND NOT has_table_privilege(current_user,'ads.session_sales_day','INSERT,UPDATE,DELETE') AS permitted''').fetchone()
            self.assertTrue(rights['permitted'])
        env = {**os.environ,'PGDATABASE':self.database,'WAREHOUSE_REPORTER_USER':self.roles['reporter'].name,
               'WAREHOUSE_REPORTER_PASSWORD':self.roles['reporter'].password,
               'WAREHOUSE_WRITER_PASSWORD':'invalid','WAREHOUSE_READER_PASSWORD':'invalid'}
        checked = subprocess.run([sys.executable,'-m','warehouse','report','--session',str(self.sessions[0])],
                                 env=env,text=True,capture_output=True,timeout=30)
        self.assertEqual(checked.returncode,0,checked.stderr)
        self.assertEqual(json.loads(checked.stdout)['totals']['paid_amount'],'220.00')

    def test_full_pipeline_and_replay_keep_ods_and_published_report(self):
        self.paid_order()
        full = self.pipeline()
        self.assertEqual(full['status'],'SUCCEEDED')
        self.assertEqual(full['stage'],'DONE')
        self.assertIsNotNone(full['publication_event_id'])
        original = self.verify(full['source_batch_id'])['manifest_sha256']
        with self.admin() as conn:
            conn.execute("UPDATE activity SET title='实时名称已变化'")
        def reject_reader():
            raise AssertionError('重跑不应连接实时来源')
        replayed = run_pipeline(source_batch_id=full['source_batch_id'],should_publish=False,data_dir=self.root,
                                reader_factory=reject_reader,writer_factory=self.writer,spark=self.spark)
        self.assertEqual(replayed['status'],'SUCCEEDED')
        self.assertNotEqual(full['transform_run_id'],replayed['transform_run_id'])
        self.assertNotEqual(full['aggregate_run_id'],replayed['aggregate_run_id'])
        self.assertEqual(full['measured_totals'],replayed['measured_totals'])
        first = AggregateStore(self.writer).get(UUID(full['aggregate_run_id']))
        second = AggregateStore(self.writer).get(UUID(replayed['aggregate_run_id']))
        self.assertEqual(first['data_sha256'],second['data_sha256'])
        self.assertEqual(self.verify(full['source_batch_id'])['manifest_sha256'],original)
        self.assertEqual(self.report()['publication']['aggregate_run_id'],full['aggregate_run_id'])

    def test_pipeline_failure_records_stage_and_keeps_published_report(self):
        self.paid_order()
        first = self.pipeline()
        with patch.object(AggregateStore,'load',side_effect=OSError('injected')):
            with self.assertRaises(WarehouseError) as caught:
                self.pipeline()
        failed = PipelineStore(self.writer).get(UUID(caught.exception.identifiers['pipeline_run_id']))
        self.assertEqual(failed['status'],'FAILED')
        self.assertEqual(failed['stage'],'AGGREGATE')
        self.assertIsNotNone(failed['source_batch_id'])
        self.assertIsNotNone(failed['transform_run_id'])
        self.assertIsNotNone(failed['aggregate_run_id'])
        self.assertEqual(self.report()['publication']['aggregate_run_id'],first['aggregate_run_id'])

    def test_pipeline_publish_failure_rolls_back_completion(self):
        self.paid_order()
        with patch('warehouse.publication._complete_pipeline',side_effect=OSError('injected')):
            with self.assertRaises(WarehouseError) as caught:
                self.pipeline()
        failed = PipelineStore(self.writer).get(UUID(caught.exception.identifiers['pipeline_run_id']))
        self.assertEqual((failed['status'],failed['stage']),('FAILED','PUBLISH'))
        self.assertEqual(self.value('SELECT generation AS value FROM wh_meta.publication'),0)

    def test_validation_only_pipeline_recovers_lost_completion_reply(self):
        self.paid_order()
        source = self.run_extract()
        original = PipelineStore.complete
        def lose_reply(store,*args):
            original(store,*args)
            raise OSError('lost reply')
        with patch.object(PipelineStore,'complete',lose_reply):
            result = self.pipeline(source_batch_id=source['source_batch_id'],should_publish=False)
        self.assertEqual(result['status'],'SUCCEEDED')
        self.assertEqual(self.value('SELECT generation AS value FROM wh_meta.publication'),0)
