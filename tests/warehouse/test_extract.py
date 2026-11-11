"""独立数据库、随机账号和临时文件验证真实快照、Parquet 及发布边界。"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
import pyarrow.parquet as pq

from support import BookingFixture
from ticketing.db import connect
from ticketing.payment import PaymentService
from warehouse.batches import BatchStore
from warehouse.bootstrap import grant_warehouse
from warehouse.config import Role
from warehouse.contracts import SOURCE_TABLES
from warehouse.errors import ExtractionError
from warehouse.extract import extract, verify_batch, _extract_table
from warehouse.manifests import sha256_file


class ExtractionTests(BookingFixture):
    @classmethod
    def setUpClass(cls):
        suffix = uuid4().hex[:12]
        cls.roles = {kind: Role(f'ods_test_{kind}_{suffix}', uuid4().hex)
                     for kind in ('reader', 'writer', 'reporter')}

        def cleanup_roles():
            with connect(dbname='postgres') as admin:
                for role in cls.roles.values():
                    admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role.name)))
        cls.addClassCleanup(cleanup_roles)
        super().setUpClass()
        with connect(dbname=cls.database) as admin:
            grant_warehouse(admin, cls.roles)

    def setUp(self):
        # 仅清理本类随机创建的独立测试库，确保每个用例具有固定的业务输入。
        self.assertTrue(self.database.startswith('ticketing_test_'))
        with self.admin() as conn:
            conn.execute('TRUNCATE public.app_user, public.venue CASCADE')
            conn.execute('TRUNCATE wh_meta.extract_batch')
        super().setUp()
        self.temporary = tempfile.TemporaryDirectory(prefix='ods-test-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def as_role(self, kind):
        role = self.roles[kind]
        return connect(dbname=self.database, user=role.name, password=role.password)

    def reader(self):
        return self.as_role('reader')

    def writer(self):
        return self.as_role('writer')

    def run_extract(self, **kwargs):
        return extract(data_dir=self.root, chunk_size=1, reader_factory=self.reader, writer_factory=self.writer, **kwargs)

    def verify(self, batch):
        return verify_batch(batch, data_dir=self.root, writer_factory=self.writer)

    def record(self, batch):
        return BatchStore(self.writer).get(UUID(str(batch)))

    def manifest(self, result):
        return json.loads((self.root / result['manifest_path']).read_text())

    def rows(self, result, table):
        return pq.read_table(self.root / 'ods' / result['source_batch_id'] / table / 'part-00000.parquet').to_pylist()

    def paid_order(self):
        order = self.reserve()
        PaymentService(self.runtime).pay(self.users[0], order['order_id'], Decimal('220.00'))
        return order

    def test_seven_tables_types_counts_and_reference_totals(self):
        order = self.paid_order()
        with self.admin() as admin:
            admin.execute("UPDATE activity SET title='中文活动', description='保留中文与换行' WHERE activity_id=%s", (self.activity,))
        result = self.run_extract()
        self.assertEqual(result['status'], 'SUCCEEDED')
        self.assertEqual(set(result['row_counts']), set(SOURCE_TABLES))
        self.assertEqual(result['row_counts']['booking_item'], 2)
        self.assertEqual(result['row_counts']['ticket'], 2)
        self.assertEqual(result['reference_totals'], {'paid_order_count': 1, 'sold_ticket_count': 2, 'paid_amount': '220.00'})
        items = self.rows(result, 'booking_item')
        self.assertEqual([i['unit_price'] for i in items], [Decimal('100.00'), Decimal('120.00')])
        orders = self.rows(result, 'booking_order')
        self.assertEqual(orders[0]['order_id'], order['order_id'])
        self.assertIsInstance(orders[0]['request_id'], str)
        self.assertEqual(UUID(orders[0]['request_id']), order['request_id'])
        self.assertIsNone(orders[0]['refunded_at'])
        self.assertEqual(orders[0]['paid_at'].utcoffset().total_seconds(), 0)
        self.assertEqual(self.rows(result, 'activity')[0]['title'], '中文活动')
        self.assertEqual(self.verify(result['source_batch_id'])['files_verified'], 7)
        for table, entry in self.manifest(result)['tables'].items():
            self.assertEqual(entry['row_count'], result['row_counts'][table])
            self.assertEqual(entry['source_row_count'], entry['row_count'])
        self.assertEqual(list((self.root / '.staging').iterdir()), [])

    def test_empty_database_has_typed_empty_files(self):
        with self.admin() as admin:
            admin.execute('TRUNCATE public.app_user, public.venue CASCADE')
        result = self.run_extract()
        self.assertTrue(all(count == 0 for count in result['row_counts'].values()))
        self.assertEqual(result['reference_totals']['paid_amount'], '0.00')
        self.assertEqual(result['reference_totals']['sold_ticket_count'], 0)
        self.assertEqual(len(self.rows(result, 'booking_order')), 0)
        self.verify(result['source_batch_id'])

    def test_refunded_sales_and_failed_attempts_remain_raw(self):
        order = self.reserve()
        service = PaymentService(self.runtime)
        service.pay(self.users[0], order['order_id'], Decimal('220'), outcome='FAILED')
        service.pay(self.users[0], order['order_id'], Decimal('220'))
        service.refund(self.users[0], order['order_id'], reason='原始层保留退款后的销售')
        result = self.run_extract()
        self.assertEqual(result['row_counts']['payment_attempt'], 2)
        self.assertEqual({r['status'] for r in self.rows(result, 'payment_attempt')}, {'FAILED', 'SUCCEEDED'})
        self.assertEqual({r['status'] for r in self.rows(result, 'ticket')}, {'REFUNDED'})
        self.assertEqual(result['reference_totals'], {'paid_order_count': 1, 'sold_ticket_count': 2, 'paid_amount': '220.00'})

    def test_zero_amount_sale_and_utc_boundary_preserved(self):
        with self.admin() as admin:
            admin.execute('UPDATE session_seat SET price=0 WHERE session_id=%s', (self.sessions[0],))
        order = self.reserve()
        PaymentService(self.runtime).pay(self.users[0], order['order_id'], Decimal('0'))
        boundary = datetime(2026, 11, 3, 16, 0, tzinfo=timezone.utc)
        with self.admin() as admin:
            admin.execute('UPDATE booking_order SET paid_at=%s WHERE order_id=%s', (boundary, order['order_id']))
        result = self.run_extract()
        self.assertEqual(result['reference_totals'], {'paid_order_count': 1, 'sold_ticket_count': 2, 'paid_amount': '0.00'})
        self.assertEqual(self.rows(result, 'booking_order')[0]['paid_at'], boundary)

    def test_payment_committing_between_table_reads_does_not_mix_snapshots(self):
        order = self.reserve()
        with ThreadPoolExecutor(max_workers=1) as pool:
            def export_then_commit(reader, spec, directory, chunk_size):
                count = _extract_table(reader, spec, directory, chunk_size)
                if spec.name == 'booking_order':
                    pool.submit(PaymentService(self.runtime).pay,
                                self.users[0], order['order_id'], Decimal('220')).result(timeout=15)
                return count
            with patch('warehouse.extract._extract_table', side_effect=export_then_commit):
                before = self.run_extract()
        self.assertEqual(self.rows(before, 'booking_order')[0]['status'], 'PENDING')
        self.assertEqual(before['row_counts']['payment_attempt'], 0)
        self.assertEqual(before['row_counts']['ticket'], 0)
        self.assertEqual(before['reference_totals']['paid_order_count'], 0)
        after = self.run_extract()
        self.assertEqual(self.rows(after, 'booking_order')[0]['status'], 'PAID')
        self.assertEqual(after['row_counts']['ticket'], 2)
        self.assertEqual(after['reference_totals']['paid_amount'], '220.00')
        self.verify(before['source_batch_id'])
        self.verify(after['source_batch_id'])

    def test_new_run_does_not_overwrite_previous_files(self):
        self.paid_order()
        first = self.run_extract()
        before = {str(p.relative_to(self.root)): sha256_file(p) for p in (self.root / 'ods' / first['source_batch_id']).rglob('*') if p.is_file()}
        second = self.run_extract()
        self.assertNotEqual(first['source_batch_id'], second['source_batch_id'])
        self.assertEqual(first['row_counts'], second['row_counts'])
        self.assertEqual(before, {p: sha256_file(self.root / p) for p in before})

    def test_uuid_collision_does_not_change_existing_success(self):
        first = self.run_extract()
        with patch('warehouse.extract.uuid4', return_value=UUID(first['source_batch_id'])):
            with self.assertRaisesRegex(ExtractionError, 'BATCH_REGISTRATION_FAILED'):
                self.run_extract()
        self.verify(first['source_batch_id'])
        self.assertEqual(self.value('SELECT count(*) AS value FROM wh_meta.extract_batch'), 1)

    def test_failure_during_export_marks_failed_and_cleans_partial_files(self):
        def fail_after_first(reader, spec, directory, chunk_size):
            count = _extract_table(reader, spec, directory, chunk_size)
            if spec.name == 'booking_item':
                raise OSError('injected failure containing secret-looking text')
            return count
        with patch('warehouse.extract._extract_table', side_effect=fail_after_first):
            with self.assertRaises(ExtractionError) as caught:
                self.run_extract()
        batch = caught.exception.batch_id
        self.assertEqual(self.record(batch)['status'], 'FAILED')
        self.assertEqual(self.record(batch)['error_code'], 'EXTRACTION_FAILED')
        self.assertEqual(list((self.root / '.staging').iterdir()), [])
        self.assertEqual(list((self.root / 'ods').iterdir()), [])
        with self.assertRaisesRegex(ExtractionError, 'BATCH_NOT_SUCCEEDED'):
            self.verify(batch)

    def test_failure_after_rename_never_publishes_or_deletes_final_directory(self):
        with patch.object(BatchStore, 'succeed', side_effect=OSError('injected failure')):
            with self.assertRaises(ExtractionError) as caught:
                self.run_extract()
        batch = caught.exception.batch_id
        self.assertTrue((self.root / 'ods' / batch / 'manifest.json').exists())
        self.assertEqual(self.record(batch)['status'], 'FAILED')
        with self.assertRaisesRegex(ExtractionError, 'BATCH_NOT_SUCCEEDED'):
            self.verify(batch)

    def test_lost_success_acknowledgement_is_resolved_without_downgrade(self):
        succeed = BatchStore.succeed

        def commit_then_lose_reply(store, *args):
            succeed(store, *args)
            raise OSError('commit acknowledgement lost')
        with patch.object(BatchStore, 'succeed', commit_then_lose_reply):
            result = self.run_extract()
        self.assertEqual(result['status'], 'SUCCEEDED')
        self.assertEqual(result['files_verified'], 7)
        self.assertIsNone(self.record(result['source_batch_id'])['error_code'])

    def test_unknown_publication_status_remains_non_consumable(self):
        with patch.object(BatchStore, 'succeed', side_effect=OSError('unavailable')):
            with patch.object(BatchStore, 'get', side_effect=OSError('unavailable')):
                with self.assertRaisesRegex(ExtractionError, 'BATCH_STATUS_UNCERTAIN') as caught:
                    self.run_extract()
        batch = caught.exception.batch_id
        self.assertEqual(self.record(batch)['status'], 'RUNNING')
        self.assertTrue((self.root / 'ods' / batch).is_dir())
        with self.assertRaisesRegex(ExtractionError, 'BATCH_NOT_SUCCEEDED'):
            self.verify(batch)

    def test_corrupt_data_file_is_rejected(self):
        result = self.run_extract()
        file = self.root / 'ods' / result['source_batch_id'] / 'venue/part-00000.parquet'
        with file.open('ab') as stream:
            stream.write(b'corruption')
        with self.assertRaisesRegex(ExtractionError, 'FILE_CHECKSUM_MISMATCH'):
            self.verify(result['source_batch_id'])

    def test_corrupt_manifest_is_rejected(self):
        result = self.run_extract()
        with (self.root / result['manifest_path']).open('a') as stream:
            stream.write('corruption')
        with self.assertRaisesRegex(ExtractionError, 'MANIFEST_CHECKSUM_MISMATCH'):
            self.verify(result['source_batch_id'])

    def test_manifest_cannot_redirect_to_another_path_even_with_matching_hash(self):
        result = self.run_extract()
        path = self.root / result['manifest_path']
        manifest = self.manifest(result)
        manifest['tables']['venue']['path'] = '../outside.parquet'
        path.write_text(json.dumps(manifest))
        with self.writer() as writer:
            writer.execute('UPDATE wh_meta.extract_batch SET manifest_sha256=%s WHERE source_batch_id=%s',
                           (sha256_file(path), UUID(result['source_batch_id'])))
        with self.assertRaisesRegex(ExtractionError, 'INVALID_MANIFEST'):
            self.verify(result['source_batch_id'])

    def test_schema_drift_stops_batch(self):
        with self.admin() as admin:
            admin.execute('ALTER TABLE venue ADD COLUMN unexpected TEXT')
        try:
            with self.assertRaisesRegex(ExtractionError, 'SOURCE_SCHEMA_MISMATCH') as caught:
                self.run_extract()
            self.assertEqual(self.record(caught.exception.batch_id)['status'], 'FAILED')
        finally:
            with self.admin() as admin:
                admin.execute('ALTER TABLE venue DROP COLUMN unexpected')

    def test_source_connection_really_is_read_only(self):
        def try_to_write(reader, spec, directory, chunk_size):
            reader.execute("UPDATE venue SET name='forbidden' WHERE false")
        with patch('warehouse.extract._extract_table', side_effect=try_to_write):
            with self.assertRaises(ExtractionError) as caught:
                self.run_extract()
        self.assertIsInstance(caught.exception.__cause__, psycopg.errors.ReadOnlySqlTransaction)

    def test_invalid_chunk_size_registers_no_batch(self):
        for size in (0, -1, 100001, True):
            with self.subTest(size=size), self.assertRaisesRegex(ExtractionError, 'INVALID_CHUNK_SIZE'):
                extract(data_dir=self.root, chunk_size=size, reader_factory=self.reader, writer_factory=self.writer)
        self.assertEqual(self.value('SELECT count(*) AS value FROM wh_meta.extract_batch'), 0)

    def test_cli_extract_and_verify_use_restricted_roles(self):
        env = {**os.environ, 'PGDATABASE': self.database, 'WAREHOUSE_DATA_DIR': str(self.root)}
        for kind, role in self.roles.items():
            env['WAREHOUSE_' + kind.upper() + '_USER'] = role.name
            env['WAREHOUSE_' + kind.upper() + '_PASSWORD'] = role.password
        result = subprocess.run([sys.executable, '-m', 'warehouse', 'extract', '--chunk-size', '1'],
                                env=env, capture_output=True, text=True, timeout=45)
        self.assertEqual(result.returncode, 0, result.stderr)
        batch = json.loads(result.stdout)['source_batch_id']
        verified = subprocess.run([sys.executable, '-m', 'warehouse', 'verify-batch', '--batch', batch],
                                  env=env, capture_output=True, text=True, timeout=45)
        self.assertEqual(verified.returncode, 0, verified.stderr)
        self.assertEqual(json.loads(verified.stdout)['files_verified'], 7)
        failed = subprocess.run([sys.executable, '-m', 'warehouse', 'extract', '--chunk-size', '0'],
                                env=env, capture_output=True, text=True, timeout=45)
        self.assertEqual(failed.returncode, 1)
        self.assertEqual(json.loads(failed.stderr)['error'], 'INVALID_CHUNK_SIZE')
        for role in self.roles.values():
            self.assertNotIn(role.password, result.stdout + result.stderr + failed.stderr)
