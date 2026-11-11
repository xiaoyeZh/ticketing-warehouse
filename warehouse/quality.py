"""只返回可控错误码；不静默去重或丢弃缺关联的销售。"""
from functools import reduce
from operator import or_
from decimal import Decimal

from pyspark.sql import functions as F

from .errors import TransformationError
from .model_schemas import MODELS
from .schemas import TABLE_SPECS


def _reject(frame, code):
    if frame.limit(1).count():
        raise TransformationError(code)


def _identity(frame, key, required, duplicate_code):
    missing = reduce(or_, (F.col(name).isNull() for name in required))
    row = frame.agg(F.count('*').alias('rows'), F.countDistinct(key).alias('keys'),
                    F.max(F.when(missing, 1).otherwise(0)).alias('missing')).first()
    if row.missing:
        raise TransformationError('NULL_REQUIRED_FIELD')
    if row.rows != row.keys:
        raise TransformationError(duplicate_code)
    return row.rows


def validate_sources(frames, expected_counts):
    for name, frame in frames.items():
        spec = TABLE_SPECS[name]
        count = _identity(frame, spec.primary_key, [c.name for c in spec.columns if not c.nullable], 'SOURCE_DUPLICATE_KEY')
        if count != expected_counts[name]:
            raise TransformationError('SOURCE_ROW_COUNT_MISMATCH')
    o, i, p, t = (frames[name] for name in ('booking_order', 'booking_item', 'payment_attempt', 'ticket'))
    s, a, v = (frames[name] for name in ('activity_session', 'activity', 'venue'))
    relations = (
        i.join(o, (i.order_id == o.order_id) & (i.session_id == o.session_id), 'left_anti'),
        o.join(s, o.session_id == s.session_id, 'left_anti'),
        p.join(o, p.order_id == o.order_id, 'left_anti'),
        t.join(i, (t.item_id == i.item_id) & (t.order_id == i.order_id)
               & (t.session_seat_id == i.session_seat_id), 'left_anti'),
        s.join(a, s.activity_id == a.activity_id, 'left_anti'),
        s.join(v, s.venue_id == v.venue_id, 'left_anti'),
    )
    for relation in relations:
        _reject(relation, 'SOURCE_RELATION_MISMATCH')
    _reject(t.groupBy('item_id').count().where('count > 1'), 'MULTIPLE_TICKETS_PER_ITEM')
    succeeded = p.where("status='SUCCEEDED'")
    _reject(succeeded.groupBy('order_id').count().where('count > 1'), 'MULTIPLE_SUCCESS_PAYMENTS')
    paid_orders = o.where('paid_at IS NOT NULL')
    _reject(paid_orders.join(succeeded.select('order_id'), 'order_id', 'left_anti'), 'MISSING_SUCCESS_PAYMENT')
    _reject(succeeded.join(o, 'order_id').where(F.col('paid_at').isNull() | (F.col('amount') != F.col('total_amount'))),
            'INVALID_SUCCESS_PAYMENT')
    item_totals = i.groupBy('order_id').agg(F.sum('unit_price').alias('item_amount'))
    _reject(paid_orders.join(item_totals, 'order_id', 'left').where(
        F.col('item_amount').isNull() | (F.col('item_amount') != F.col('total_amount'))), 'ORDER_AMOUNT_MISMATCH')
    paid_items = i.join(paid_orders.select('order_id'), 'order_id')
    _reject(paid_items.join(t.select('item_id'), 'item_id', 'left_anti'), 'MISSING_ISSUED_TICKET')
    _reject(t.join(paid_items.select('item_id'), 'item_id', 'left_anti'), 'UNPAID_TICKET')


def validate_models(frames, reference_totals, session_count):
    counts = {name: _identity(frame, MODELS[name].primary_key,
                             [field for field, _ in MODELS[name].fields], 'MODEL_DUPLICATE_KEY')
              for name, frame in frames.items()}
    if counts['dim_session'] != session_count:
        raise TransformationError('SESSION_DIMENSION_MISMATCH')
    fact, days, sessions = (frames[name] for name in ('dwd_ticket_sale', 'dim_date', 'dim_session'))
    _reject(fact.where((F.col('sale_amount') < 0) | (F.col('sale_quantity') != 1)), 'INVALID_SALE_ROW')
    _reject(fact.join(sessions.select('session_id'), 'session_id', 'left_anti'), 'MODEL_RELATION_MISMATCH')
    _reject(fact.join(days, fact.paid_date_key == days.date_key, 'left_anti'), 'MODEL_RELATION_MISMATCH')
    _reject(sessions.join(days, sessions.start_date_key == days.date_key, 'left_anti'), 'MODEL_RELATION_MISMATCH')
    row = fact.agg(F.count('*').alias('tickets'), F.countDistinct('order_id').alias('orders'),
                    F.sum('sale_amount').alias('amount')).first()
    totals = {'sold_ticket_count': row.tickets, 'paid_order_count': row.orders,
              'paid_amount': format(row.amount if row.amount is not None else Decimal('0.00'), '.2f')}
    if totals != reference_totals:
        raise TransformationError('RECONCILIATION_MISMATCH')
    return counts, totals
