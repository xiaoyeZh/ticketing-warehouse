"""只用 reporter 查询 ADS；元信息、明细与合计来自同一只读数据库快照。"""
from datetime import date
from decimal import Decimal

from .aggregate_runs import scalar
from .config import connect_role
from .errors import WarehouseError


def report(*, start_date=None, end_date=None, session_id=None, reporter_factory=None):
    start_date = date.fromisoformat(str(start_date)) if start_date is not None else None
    end_date = date.fromisoformat(str(end_date)) if end_date is not None else None
    if (start_date is not None and end_date is not None and start_date >= end_date) or (
            session_id is not None and session_id <= 0):
        raise WarehouseError('INVALID_REPORT_FILTER')
    connect = reporter_factory or (lambda: connect_role('reporter'))
    with connect() as conn, conn.transaction():
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        publication = conn.execute('SELECT * FROM ads.session_sales_publication').fetchone()
        if not publication or publication['aggregate_run_id'] is None:
            raise WarehouseError('REPORT_NOT_PUBLISHED')
        conditions = []
        parameters = []
        for value, condition in ((start_date, 'paid_date >= %s'), (end_date, 'paid_date < %s'),
                                 (session_id, 'session_id = %s')):
            if value is not None:
                conditions.append(condition)
                parameters.append(value)
        where = (' WHERE ' + ' AND '.join(conditions)) if conditions else ''
        rows = conn.execute('''SELECT paid_date,session_id,activity_id,activity_title,venue_id,venue_name,
            session_start_at,sold_ticket_count,paid_order_count,paid_amount FROM ads.session_sales_day'''
            + where + ' ORDER BY paid_date,session_id', parameters).fetchall()
        totals = conn.execute('''SELECT COALESCE(sum(sold_ticket_count),0) AS sold_ticket_count,
            COALESCE(sum(paid_order_count),0) AS paid_order_count,COALESCE(sum(paid_amount),0.00) AS paid_amount
            FROM ads.session_sales_day''' + where, parameters).fetchone()
    return {'status': 'SUCCEEDED', 'publication': {key: scalar(value) if not key.endswith('_id') else str(value)
            for key, value in publication.items() if key not in ('measured_totals', 'row_count')},
            'filters': {'start_date': scalar(start_date), 'end_date_exclusive': scalar(end_date), 'session_id': session_id},
            'rows': [{key: scalar(value) for key, value in row.items()} for row in rows],
            'totals': {'sold_ticket_count': int(totals['sold_ticket_count']),
                       'paid_order_count': int(totals['paid_order_count']),
                       'paid_amount': format(Decimal(totals['paid_amount']), '.2f')}}
