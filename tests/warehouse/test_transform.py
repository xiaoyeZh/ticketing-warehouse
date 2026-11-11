"""真实 Spark 加工，对预期业务结果、质量失败、血缘与交付边界作独立验证。"""
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import os
import subprocess
import sys
from unittest.mock import patch
from uuid import UUID

from psycopg.types.json import Jsonb
import pyarrow as pa
import pyarrow.parquet as pq

from warehouse_fixture import WarehouseFixture
from ticketing.payment import PaymentService
from warehouse.errors import TransformationError
from warehouse.manifests import sha256_file
from warehouse.schemas import TABLE_SPECS
from warehouse.spark import create_session
from warehouse.transform import transform, verify_transform, _write_outputs
from warehouse.transform_runs import TransformStore


class TransformationTests(WarehouseFixture):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark = create_session('ticketing-transform-integration-tests')
        cls.addClassCleanup(cls.spark.stop)

    def run_transform(self, source):
        return transform(source['source_batch_id'], data_dir=self.root, writer_factory=self.writer, spark=self.spark)

    def model_rows(self, result, name):
        path = self.root / 'curated' / result['source_batch_id'] / result['run_id'] / name
        return pq.read_table(path).to_pylist()

    def verify_model(self, result):
        return verify_transform(result['run_id'], data_dir=self.root, writer_factory=self.writer)

    def rewrite_source(self, result, name, rows):
        # 构造物理清单有效但业务质量不合格的输入，验证加工不会依赖源库约束。
        file = self.root / 'ods' / result['source_batch_id'] / name / 'part-00000.parquet'
        pq.write_table(pa.Table.from_pylist(rows, schema=TABLE_SPECS[name].arrow_schema), file)
        manifest = self.manifest(result)
        manifest['tables'][name].update(row_count=len(rows), source_row_count=len(rows),
                                        size_bytes=file.stat().st_size, sha256=sha256_file(file))
        path = self.root / result['manifest_path']
        path.write_text(json.dumps(manifest))
        counts = {table: entry['row_count'] for table, entry in manifest['tables'].items()}
        with self.writer() as conn:
            conn.execute('UPDATE wh_meta.extract_batch SET manifest_sha256=%s,row_counts=%s WHERE source_batch_id=%s',
                         (sha256_file(path), Jsonb(counts), UUID(result['source_batch_id'])))
        self.verify(result['source_batch_id'])

    def test_normal_sale_dimensions_and_decimal(self):
        self.paid_order()
        source = self.run_extract()
        result = self.run_transform(source)
        self.assertEqual(result['status'], 'SUCCEEDED')
        self.assertEqual(result['row_counts']['dwd_ticket_sale'], 2)
        self.assertEqual(result['row_counts']['dim_session'], 2)
        self.assertEqual(result['measured_totals'], {'sold_ticket_count': 2, 'paid_order_count': 1, 'paid_amount': '220.00'})
        rows = sorted(self.model_rows(result, 'dwd_ticket_sale'), key=lambda r:r['item_id'])
        self.assertEqual([r['sale_amount'] for r in rows], [Decimal('100.00'), Decimal('120.00')])
        self.assertEqual([r['sale_quantity'] for r in rows], [1,1])
        self.assertEqual({r['source_batch_id'] for r in rows}, {source['source_batch_id']})
        self.assertEqual({r['transform_run_id'] for r in rows}, {result['run_id']})
        self.assertEqual(self.verify_model(result)['tables_verified'], 3)

    def test_refund_resale_and_price_snapshots(self):
        order = self.reserve()
        service = PaymentService(self.runtime)
        service.pay(self.users[0], order['order_id'], Decimal('220'), outcome='FAILED')
        service.pay(self.users[0], order['order_id'], Decimal('220'))
        service.refund(self.users[0], order['order_id'], reason='重售')
        with self.admin() as conn:
            for seat, price in zip(self.inventory[0], (150,125)):
                conn.execute('UPDATE session_seat SET price=%s WHERE session_seat_id=%s', (price,seat))
        resale = self.reserve(user=1)
        service.pay(self.users[1], resale['order_id'], Decimal('275'))
        source = self.run_extract()
        result = self.run_transform(source)
        self.assertEqual(result['measured_totals'], {'sold_ticket_count':4,'paid_order_count':2,'paid_amount':'495.00'})
        rows = self.model_rows(result, 'dwd_ticket_sale')
        self.assertEqual(sum(r['sale_amount'] for r in rows if r['order_id']==order['order_id']), Decimal('220'))
        self.assertEqual(len({r['ticket_id'] for r in rows}), 4)
        self.assertEqual(len({r['session_seat_id'] for r in rows}), 2)

    def test_zero_amount_and_shanghai_midnight(self):
        with self.admin() as conn:
            conn.execute('UPDATE session_seat SET price=0 WHERE session_id=%s', (self.sessions[0],))
        boundaries = (datetime(2026,11,3,15,59,tzinfo=timezone.utc), datetime(2026,11,3,16,0,tzinfo=timezone.utc))
        for seat, paid_at in zip(self.inventory[0], boundaries):
            order = self.reserve(seats=[seat])
            PaymentService(self.runtime).pay(self.users[0], order['order_id'], Decimal('0'))
            with self.admin() as conn:
                conn.execute('UPDATE booking_order SET paid_at=%s WHERE order_id=%s', (paid_at,order['order_id']))
        result = self.run_transform(self.run_extract())
        self.assertEqual(result['measured_totals'], {'sold_ticket_count':2,'paid_order_count':2,'paid_amount':'0.00'})
        self.assertEqual({r['paid_date_key'] for r in self.model_rows(result,'dwd_ticket_sale')}, {20261103,20261104})
        days = {r['date_key']: r for r in self.model_rows(result,'dim_date')}
        self.assertEqual(days[20261103]['calendar_date'], date(2026,11,3))
        self.assertEqual(days[20261103]['iso_weekday'], date(2026,11,3).isoweekday())

    def test_empty_ods_produces_three_typed_empty_tables(self):
        with self.admin() as conn:
            conn.execute('TRUNCATE public.app_user,public.venue CASCADE')
        result = self.run_transform(self.run_extract())
        self.assertEqual(result['row_counts'], {'dim_date':0,'dim_session':0,'dwd_ticket_sale':0})
        self.assertEqual(result['measured_totals'], {'sold_ticket_count':0,'paid_order_count':0,'paid_amount':'0.00'})
        self.verify_model(result)

    def test_replay_uses_ods_without_reading_live_business_tables(self):
        self.paid_order()
        source = self.run_extract()
        first = self.run_transform(source)
        with self.admin() as conn:
            conn.execute("UPDATE activity SET title='后来的名称'")
        with patch.dict(os.environ, {'WAREHOUSE_READER_PASSWORD':'not-a-valid-reader-password'}):
            second = self.run_transform(source)
        self.assertNotEqual(first['run_id'],second['run_id'])
        self.assertEqual(first['code_sha256'],second['code_sha256'])
        for name in first['row_counts']:
            business = lambda result: sorted([json.dumps({k:str(v) for k,v in r.items() if k!='transform_run_id'},sort_keys=True)
                                              for r in self.model_rows(result,name)])
            self.assertEqual(business(first),business(second))
        self.verify(source['source_batch_id'])

    def test_bad_source_checksum_fails_before_run_registration(self):
        source = self.run_extract()
        file = self.root/'ods'/source['source_batch_id']/'venue/part-00000.parquet'
        with file.open('ab') as stream:
            stream.write(b'corruption')
        with self.assertRaisesRegex(TransformationError,'FILE_CHECKSUM_MISMATCH'):
            self.run_transform(source)
        self.assertEqual(self.value('SELECT count(*) AS value FROM wh_meta.transform_run'),0)

    def test_duplicate_ticket_key_fails(self):
        self.paid_order()
        source = self.run_extract()
        rows = self.rows(source,'ticket')
        self.rewrite_source(source,'ticket',rows+[rows[0]])
        with self.assertRaisesRegex(TransformationError,'SOURCE_DUPLICATE_KEY'):
            self.run_transform(source)

    def test_orphan_item_is_not_silently_dropped(self):
        self.paid_order()
        source = self.run_extract()
        rows = self.rows(source,'booking_item')
        rows[0]['order_id'] = 99999999
        self.rewrite_source(source,'booking_item',rows)
        with self.assertRaisesRegex(TransformationError,'SOURCE_RELATION_MISMATCH'):
            self.run_transform(source)

    def test_multiple_successful_payments_fail(self):
        self.paid_order()
        source = self.run_extract()
        rows = self.rows(source,'payment_attempt')
        rows.append({**rows[0],'payment_id':rows[0]['payment_id']+1000})
        self.rewrite_source(source,'payment_attempt',rows)
        with self.assertRaisesRegex(TransformationError,'MULTIPLE_SUCCESS_PAYMENTS'):
            self.run_transform(source)

    def test_order_amount_mismatch_fails(self):
        self.paid_order()
        source = self.run_extract()
        rows = self.rows(source,'booking_item')
        rows[0]['unit_price'] = Decimal('1.00')
        self.rewrite_source(source,'booking_item',rows)
        with self.assertRaisesRegex(TransformationError,'ORDER_AMOUNT_MISMATCH'):
            self.run_transform(source)

    def test_wrong_reference_total_fails_reconciliation(self):
        self.paid_order()
        source = self.run_extract()
        manifest = self.manifest(source)
        manifest['reference_totals']['paid_amount']='999.00'
        path = self.root/source['manifest_path']
        path.write_text(json.dumps(manifest))
        with self.writer() as conn:
            conn.execute('UPDATE wh_meta.extract_batch SET manifest_sha256=%s,reference_totals=%s WHERE source_batch_id=%s',
                         (sha256_file(path),Jsonb(manifest['reference_totals']),UUID(source['source_batch_id'])))
        with self.assertRaisesRegex(TransformationError,'RECONCILIATION_MISMATCH'):
            self.run_transform(source)

    def test_failure_after_rename_is_not_consumable(self):
        self.paid_order()
        with patch.object(TransformStore,'succeed',side_effect=OSError('injected')):
            with self.assertRaises(TransformationError) as caught:
                self.run_transform(self.run_extract())
        row = TransformStore(self.writer).get(UUID(caught.exception.run_id))
        self.assertEqual(row['status'],'FAILED')
        with self.assertRaisesRegex(TransformationError,'TRANSFORM_NOT_SUCCEEDED'):
            verify_transform(row['run_id'],data_dir=self.root,writer_factory=self.writer)

    def test_lost_success_acknowledgement_preserves_success(self):
        self.paid_order()
        original = TransformStore.succeed
        def lose_reply(store,*args):
            original(store,*args)
            raise OSError('lost acknowledgement')
        with patch.object(TransformStore,'succeed',lose_reply):
            result = self.run_transform(self.run_extract())
        self.assertEqual(result['status'],'SUCCEEDED')
        self.assertEqual(result['tables_verified'],3)

    def test_changed_ods_during_processing_is_not_committed(self):
        self.paid_order()
        source = self.run_extract()
        def change_after_write(frames,directory):
            _write_outputs(frames,directory)
            with (self.root/'ods'/source['source_batch_id']/'venue/part-00000.parquet').open('ab') as stream:
                stream.write(b'changed')
        with patch('warehouse.transform._write_outputs',side_effect=change_after_write):
            with self.assertRaisesRegex(TransformationError,'FILE_CHECKSUM_MISMATCH') as caught:
                self.run_transform(source)
        self.assertEqual(TransformStore(self.writer).get(UUID(caught.exception.run_id))['status'],'FAILED')

    def test_corrupt_output_and_cli_verification(self):
        self.paid_order()
        source=self.run_extract()
        result=self.run_transform(source)
        env={**os.environ,'PGDATABASE':self.database,'WAREHOUSE_DATA_DIR':str(self.root),
             'WAREHOUSE_WRITER_USER':self.roles['writer'].name,'WAREHOUSE_WRITER_PASSWORD':self.roles['writer'].password}
        checked=subprocess.run([sys.executable,'-m','warehouse','verify-transform','--run',result['run_id']],
                               env=env,text=True,capture_output=True,timeout=45)
        self.assertEqual(checked.returncode,0,checked.stderr)
        self.assertEqual(json.loads(checked.stdout)['tables_verified'],3)
        file=next((self.root/'curated'/source['source_batch_id']/result['run_id']/'dwd_ticket_sale').glob('*.parquet'))
        with file.open('ab') as stream:
            stream.write(b'corruption')
        with self.assertRaisesRegex(TransformationError,'MODEL_FILE_CHECKSUM_MISMATCH'):
            self.verify_model(result)
