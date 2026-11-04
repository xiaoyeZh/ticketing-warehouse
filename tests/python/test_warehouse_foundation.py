"""真实 PostgreSQL 验证数仓账号与未来对象默认授权；账号和数据库均独立。"""
from uuid import uuid4

import psycopg
from psycopg import sql

from support import BookingFixture
from ticketing.db import connect
from warehouse.bootstrap import grant_warehouse
from warehouse.config import Role
from warehouse.contracts import SOURCE_TABLES


class WarehouseFoundationTests(BookingFixture):
    @classmethod
    def setUpClass(cls):
        suffix = uuid4().hex[:12]
        cls.roles = {kind: Role(f'wh_test_{kind}_{suffix}', uuid4().hex)
                     for kind in ('reader', 'writer', 'reporter')}

        def cleanup_roles():
            with connect(dbname='postgres') as admin:
                for role in cls.roles.values():
                    admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role.name)))
        # 父类稍后登记数据库清理；LIFO 先删数据库，再删本类专用账号。
        cls.addClassCleanup(cleanup_roles)
        super().setUpClass()
        with connect(dbname=cls.database) as admin:
            grant_warehouse(admin, cls.roles)
            admin.execute('CREATE TABLE wh_meta.permission_probe (value TEXT NOT NULL)')
            admin.execute('''CREATE TABLE dws.permission_probe (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, value TEXT NOT NULL)''')
            admin.execute('CREATE VIEW ads.permission_probe AS SELECT id, value FROM dws.permission_probe')

    def as_role(self, kind):
        role = self.roles[kind]
        return connect(dbname=self.database, user=role.name, password=role.password,
                       application_name='warehouse-permission-test')

    def test_reader_has_only_source_reads(self):
        with self.as_role('reader') as reader:
            for table in SOURCE_TABLES:
                reader.execute(sql.SQL('SELECT * FROM public.{} LIMIT 0').format(sql.Identifier(table)))
            for statement in (
                'SELECT * FROM auth_credential LIMIT 0',
                'SELECT * FROM auth_session LIMIT 0',
                'SELECT * FROM app_user LIMIT 0',
                'SELECT * FROM refund_attempt LIMIT 0',
                "UPDATE booking_order SET status='PENDING' WHERE false",
                'CREATE TABLE public.wh_forbidden (id INT)',
                'SELECT * FROM dws.permission_probe',
            ):
                with self.subTest(statement=statement), self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    reader.execute(statement)

    def test_writer_can_change_new_warehouse_tables_but_not_business(self):
        with self.as_role('writer') as writer:
            writer.execute("INSERT INTO wh_meta.permission_probe VALUES ('run')")
            writer.execute("UPDATE wh_meta.permission_probe SET value='done'")
            row = writer.execute("INSERT INTO dws.permission_probe (value) VALUES ('summary') RETURNING id").fetchone()
            self.assertGreater(row['id'], 0)
            writer.execute('DELETE FROM dws.permission_probe')
            writer.execute('DELETE FROM wh_meta.permission_probe')
            for statement in ('SELECT * FROM booking_order LIMIT 0',
                              "UPDATE booking_order SET status='PENDING' WHERE false",
                              'CREATE TABLE dws.wh_forbidden (id INT)',
                              'SELECT * FROM ads.permission_probe'):
                with self.subTest(statement=statement), self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    writer.execute(statement)

    def test_reporter_reads_view_without_underlying_table_access(self):
        with self.as_role('reporter') as reporter:
            reporter.execute('SELECT * FROM ads.permission_probe').fetchall()
            for statement in ('SELECT * FROM dws.permission_probe',
                              'SELECT * FROM booking_order LIMIT 0',
                              'DELETE FROM ads.permission_probe',
                              'CREATE TABLE ads.wh_forbidden (id INT)'):
                with self.subTest(statement=statement), self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    reporter.execute(statement)

    def test_bootstrap_is_repeatable_and_roles_are_restricted(self):
        with self.admin() as admin:
            grant_warehouse(admin, self.roles)
            rows = admin.execute('''SELECT rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
                                    FROM pg_roles WHERE rolname=ANY(%s)''',
                                 ([role.name for role in self.roles.values()],)).fetchall()
            self.assertEqual(len(rows), 3)
            self.assertFalse(any(value for row in rows for value in row.values()))
        for kind in self.roles:
            with self.as_role(kind) as conn:
                self.assertEqual(conn.execute('SELECT current_user AS name').fetchone()['name'], self.roles[kind].name)

    def test_rejects_unmanaged_role_and_rolls_back_password_changes(self):
        reporter = self.roles['reporter']
        with self.admin() as admin:
            admin.execute(sql.SQL('COMMENT ON ROLE {} IS NULL').format(sql.Identifier(reporter.name)))
            altered = {kind: Role(role.name, uuid4().hex) for kind, role in self.roles.items()}
            try:
                with self.assertRaisesRegex(RuntimeError, '不符合本项目'):
                    grant_warehouse(admin, altered)
            finally:
                admin.execute(sql.SQL('COMMENT ON ROLE {} IS {}').format(
                    sql.Identifier(reporter.name), sql.Literal('ticketing-warehouse managed reporter')))
        # reader 和 writer 的密码更新也应随同事务回滚。
        for kind in self.roles:
            with self.as_role(kind) as conn:
                conn.execute('SELECT 1')
