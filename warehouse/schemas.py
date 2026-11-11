"""ODS v1 明确列名、PostgreSQL 类型、空值和 Arrow 类型；不使用数据推断 schema。"""
from dataclasses import dataclass
from uuid import UUID

import pyarrow as pa
from psycopg import sql

from .contracts import SOURCE_TABLES
from .errors import ExtractionError


SCHEMA_VERSION = 'ods-v1'


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str
    nullable: bool = False
    precision: int | None = None
    scale: int | None = None

    @property
    def arrow_type(self):
        if self.pg_type == 'int8':
            return pa.int64()
        if self.pg_type == 'numeric':
            return pa.decimal128(self.precision, self.scale)
        if self.pg_type == 'timestamptz':
            return pa.timestamp('us', tz='UTC')
        return pa.string()


@dataclass(frozen=True)
class TableSpec:
    name: str
    primary_key: str
    columns: tuple[Column, ...]

    @property
    def arrow_schema(self):
        return pa.schema([pa.field(c.name, c.arrow_type, nullable=c.nullable) for c in self.columns])

    @property
    def query(self):
        return sql.SQL('SELECT {} FROM public.{} ORDER BY {}').format(
            sql.SQL(', ').join(sql.Identifier(c.name) for c in self.columns),
            sql.Identifier(self.name), sql.Identifier(self.primary_key))

    def normalize(self, rows):
        return [{c.name: str(row[c.name]) if isinstance(row[c.name], UUID) else row[c.name]
                 for c in self.columns} for row in rows]

    def description(self):
        return [{'name': c.name, 'type': str(c.arrow_type), 'nullable': c.nullable} for c in self.columns]

    def validate_source(self, conn):
        rows = conn.execute('''SELECT column_name, udt_name, is_nullable, numeric_precision, numeric_scale
            FROM information_schema.columns WHERE table_schema='public' AND table_name=%s''', (self.name,)).fetchall()
        by_name = {row['column_name']: row for row in rows}
        if set(by_name) != {c.name for c in self.columns}:
            raise ExtractionError('SOURCE_SCHEMA_MISMATCH')
        for c in self.columns:
            row = by_name[c.name]
            if (row['udt_name'] != c.pg_type or (row['is_nullable'] == 'YES') != c.nullable
                    or (c.pg_type == 'numeric' and (row['numeric_precision'], row['numeric_scale']) != (c.precision, c.scale))):
                raise ExtractionError('SOURCE_SCHEMA_MISMATCH')


def bigint(name):
    return Column(name, 'int8')


def text(name, pg_type='varchar'):
    return Column(name, pg_type)


def stamp(name, nullable=False):
    return Column(name, 'timestamptz', nullable)


def money(name, precision):
    return Column(name, 'numeric', precision=precision, scale=2)


TABLE_SPECS = {s.name: s for s in (
    TableSpec('booking_order', 'order_id', (
        bigint('order_id'), text('order_no'), bigint('user_id'), bigint('session_id'),
        text('request_id', 'uuid'), money('total_amount', 14), text('status'), stamp('expires_at'),
        stamp('created_at'), stamp('updated_at'), stamp('paid_at', True), stamp('refunded_at', True))),
    TableSpec('booking_item', 'item_id', (
        bigint('item_id'), bigint('order_id'), bigint('session_id'), bigint('session_seat_id'),
        money('unit_price', 12), stamp('created_at'))),
    TableSpec('payment_attempt', 'payment_id', (
        bigint('payment_id'), text('payment_no'), bigint('order_id'), text('request_id', 'uuid'),
        money('amount', 14), text('provider'), text('status'), stamp('created_at'))),
    TableSpec('ticket', 'ticket_id', (
        bigint('ticket_id'), text('ticket_no', 'uuid'), bigint('item_id'), bigint('order_id'),
        bigint('session_seat_id'), text('status'), stamp('issued_at'), stamp('refunded_at', True))),
    TableSpec('activity_session', 'session_id', (
        bigint('session_id'), bigint('activity_id'), bigint('venue_id'), stamp('start_time'),
        stamp('end_time'), text('status'), stamp('created_at'), stamp('updated_at'))),
    TableSpec('activity', 'activity_id', (
        bigint('activity_id'), text('title'), text('description', 'text'), bigint('organizer_id'),
        text('status'), stamp('created_at'), stamp('updated_at'))),
    TableSpec('venue', 'venue_id', (
        bigint('venue_id'), text('name'), text('address', 'text'), text('status'),
        stamp('created_at'), stamp('updated_at'))),
)}

if set(TABLE_SPECS) != set(SOURCE_TABLES):
    raise RuntimeError('ODS schema 与来源契约不一致')
