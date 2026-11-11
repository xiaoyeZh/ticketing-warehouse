"""抽取与加工共用的隔离数据库、随机角色和 ODS 夹具。"""
from decimal import Decimal
import json
from pathlib import Path
import tempfile
from uuid import UUID, uuid4

from psycopg import sql
import pyarrow.parquet as pq

from support import BookingFixture
from ticketing.db import connect
from ticketing.payment import PaymentService
from warehouse.batches import BatchStore
from warehouse.bootstrap import grant_warehouse
from warehouse.config import Role
from warehouse.extract import extract, verify_batch


class WarehouseFixture(BookingFixture):
    @classmethod
    def setUpClass(cls):
        suffix = uuid4().hex[:12]
        cls.roles = {kind: Role(f'wh_fixture_{kind}_{suffix}', uuid4().hex)
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
        self.assertTrue(self.database.startswith('ticketing_test_'))
        with self.admin() as conn:
            conn.execute('TRUNCATE public.app_user, public.venue CASCADE')
            conn.execute('TRUNCATE wh_meta.extract_batch CASCADE')
        super().setUp()
        self.temporary = tempfile.TemporaryDirectory(prefix='warehouse-test-')
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
