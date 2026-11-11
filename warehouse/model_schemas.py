"""sales-dwd-v1 的逻辑类型与非空约定；Spark 磁盘 schema 的 nullable 由表达式推导。"""
from dataclasses import dataclass

import pyarrow as pa


MODEL_VERSION = 'sales-dwd-v1'


@dataclass(frozen=True)
class ModelSpec:
    name: str
    primary_key: str
    fields: tuple

    def description(self):
        return [{'name': name, 'type': str(kind), 'nullable': False} for name, kind in self.fields]


LINEAGE = (('source_batch_id', pa.string()), ('transform_run_id', pa.string()))
MODELS = {spec.name: spec for spec in (
    ModelSpec('dim_date', 'date_key', (
        ('date_key', pa.int32()), ('calendar_date', pa.date32()), ('calendar_year', pa.int32()),
        ('quarter', pa.int32()), ('month', pa.int32()), ('day_of_month', pa.int32()),
        ('iso_weekday', pa.int32()), ('iso_week_number', pa.int32()), ('is_weekend', pa.bool_()), *LINEAGE)),
    ModelSpec('dim_session', 'session_id', (
        ('session_id', pa.int64()), ('activity_id', pa.int64()), ('activity_title', pa.string()),
        ('venue_id', pa.int64()), ('venue_name', pa.string()), ('venue_address', pa.string()),
        ('organizer_id', pa.int64()), ('start_at', pa.timestamp('us', tz='UTC')),
        ('end_at', pa.timestamp('us', tz='UTC')), ('start_date', pa.date32()), ('start_date_key', pa.int32()),
        ('session_status', pa.string()), *LINEAGE)),
    ModelSpec('dwd_ticket_sale', 'ticket_id', (
        ('ticket_id', pa.int64()), ('item_id', pa.int64()), ('order_id', pa.int64()), ('payment_id', pa.int64()),
        ('session_seat_id', pa.int64()), ('session_id', pa.int64()), ('user_id', pa.int64()),
        ('paid_at', pa.timestamp('us', tz='UTC')), ('paid_date', pa.date32()), ('paid_date_key', pa.int32()),
        ('sale_quantity', pa.int32()), ('sale_amount', pa.decimal128(18, 2)), *LINEAGE)),
)}
