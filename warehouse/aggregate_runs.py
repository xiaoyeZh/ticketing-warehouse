"""版本化 DWS；装载、数据库复核与成功状态在同一个事务中提交。"""
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json

from psycopg.types.json import Jsonb

from .config import connect_role
from .errors import WarehouseError


FIELDS = ('paid_date', 'session_id', 'activity_id', 'activity_title', 'venue_id', 'venue_name',
          'session_start_at', 'sold_ticket_count', 'paid_order_count', 'paid_amount')
ZERO = {'sold_ticket_count': 0, 'paid_order_count': 0, 'paid_amount': '0.00'}


def scalar(value):
    if isinstance(value, Decimal):
        return format(value, '.2f')
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def measure(rows):
    digest = hashlib.sha256()
    tickets = orders = count = 0
    amount = Decimal('0.00')
    for row in rows:
        digest.update(json.dumps({key: scalar(row[key]) for key in FIELDS},
                                 sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode() + b'\n')
        count += 1
        tickets += row['sold_ticket_count']
        orders += row['paid_order_count']
        amount += row['paid_amount']
    return count, {'sold_ticket_count': tickets, 'paid_order_count': orders, 'paid_amount': format(amount, '.2f')}, digest.hexdigest()


def get_run(conn, run_id, *, lock=False):
    row = conn.execute('SELECT * FROM wh_meta.aggregate_run WHERE run_id=%s' + (' FOR UPDATE' if lock else ''),
                       (run_id,)).fetchone()
    if not row:
        raise WarehouseError('AGGREGATE_NOT_FOUND', aggregate_run_id=run_id)
    return row


def check_loaded(conn, record):
    if record['status'] != 'SUCCEEDED':
        raise WarehouseError('AGGREGATE_NOT_SUCCEEDED')
    source = conn.execute('''SELECT t.manifest_sha256,t.source_batch_id,t.measured_totals,t.contract_version,
        b.reference_totals,b.snapshot_recorded_at FROM wh_meta.transform_run t
        JOIN wh_meta.extract_batch b ON b.source_batch_id=t.source_batch_id
        WHERE t.run_id=%s AND t.status='SUCCEEDED' AND b.status='SUCCEEDED'
          AND t.input_manifest_sha256=b.manifest_sha256''', (record['transform_run_id'],)).fetchone()
    if (not source or source['source_batch_id'] != record['source_batch_id']
            or source['manifest_sha256'] != record['input_manifest_sha256']
            or source['contract_version'] != record['contract_version']
            or record['contract_version'] != 'sales-v1' or record['schema_version'] != 'sales-dws-v1'):
        raise WarehouseError('AGGREGATE_LINEAGE_MISMATCH')
    rows = conn.execute('SELECT ' + ','.join(FIELDS) + ''' FROM dws.session_sales_day
        WHERE aggregate_run_id=%s ORDER BY paid_date,session_id''', (record['run_id'],)).fetchall()
    count, totals, checksum = measure(rows)
    if (count != record['row_count'] or totals != record['measured_totals']
            or totals != source['measured_totals'] or totals != source['reference_totals']
            or checksum != record['data_sha256']):
        raise WarehouseError('AGGREGATE_DATA_MISMATCH')
    return source


class AggregateStore:
    def __init__(self, connect=None):
        self.connect = connect or (lambda: connect_role('writer'))

    def register(self, run_id, model, fingerprint):
        with self.connect() as conn:
            row = conn.execute('''INSERT INTO wh_meta.aggregate_run
                (run_id,transform_run_id,source_batch_id,input_manifest_sha256,code_sha256,contract_version,schema_version)
                SELECT %s,run_id,source_batch_id,manifest_sha256,%s,contract_version,'sales-dws-v1'
                FROM wh_meta.transform_run WHERE run_id=%s AND status='SUCCEEDED' AND manifest_sha256=%s
                RETURNING run_id''', (run_id, fingerprint, model['run_id'], model['manifest_sha256'])).fetchone()
        if not row:
            raise WarehouseError('TRANSFORM_CHANGED')

    def load(self, run_id, rows, totals):
        count, actual, checksum = measure(rows)
        if actual != totals:
            raise WarehouseError('AGGREGATE_RECONCILIATION_MISMATCH')
        with self.connect() as conn, conn.transaction():
            record = get_run(conn, run_id, lock=True)
            if record['status'] != 'RUNNING':
                raise WarehouseError('AGGREGATE_STATE_CONFLICT')
            with conn.cursor() as cursor:
                cursor.executemany('INSERT INTO dws.session_sales_day (aggregate_run_id,' + ','.join(FIELDS)
                                   + ') VALUES (' + ','.join(['%s'] * (len(FIELDS) + 1)) + ')',
                                   [(run_id, *[row[field] for field in FIELDS]) for row in rows])
            record = conn.execute('''UPDATE wh_meta.aggregate_run SET status='SUCCEEDED',finished_at=clock_timestamp(),
                row_count=%s,measured_totals=%s,data_sha256=%s WHERE run_id=%s RETURNING *''',
                (count, Jsonb(totals), checksum, run_id)).fetchone()
            check_loaded(conn, record)
        return record

    def get(self, run_id):
        with self.connect() as conn:
            return get_run(conn, run_id)

    def fail(self, run_id, code):
        with self.connect() as conn:
            conn.execute('''UPDATE wh_meta.aggregate_run SET status='FAILED',finished_at=clock_timestamp(),error_code=%s
                WHERE run_id=%s AND status='RUNNING' ''', (code, run_id))
