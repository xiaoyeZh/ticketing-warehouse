"""发布时锁定唯一指针；并发保护、历史事件和流水线完成同事务提交。"""
from uuid import UUID, uuid4

from .aggregate_runs import AggregateStore, check_loaded, get_run
from .errors import WarehouseError


def current(conn, *, lock=False):
    return conn.execute('''SELECT p.generation,p.event_id,e.run_id,e.published_at
        FROM wh_meta.publication p LEFT JOIN wh_meta.publication_event e ON e.event_id=p.event_id
        WHERE p.name='session_sales_day' ''' + ('FOR UPDATE OF p' if lock else '')).fetchone()


def _complete_pipeline(conn, pipeline_id, run_id, event_id):
    if pipeline_id is not None:
        row = conn.execute('''UPDATE wh_meta.pipeline_run SET status='SUCCEEDED',stage='DONE',
            publication_event_id=%s,finished_at=clock_timestamp()
            WHERE run_id=%s AND status='RUNNING' AND aggregate_run_id=%s AND publish_requested
            RETURNING run_id''', (event_id, pipeline_id, run_id)).fetchone()
        if not row:
            raise WarehouseError('PIPELINE_STATE_CONFLICT')


def _result(event, record):
    return {'status': 'PUBLISHED', 'event_id': str(event['event_id']), 'generation': event['generation'],
            'aggregate_run_id': str(record['run_id']), 'source_batch_id': str(record['source_batch_id']),
            'published_at': event['published_at'].isoformat(), 'measured_totals': record['measured_totals']}


def publish(run_id, *, expected_generation=None, allow_older=False, writer_factory=None, pipeline_id=None):
    run_id = UUID(str(run_id))
    if expected_generation is not None and expected_generation < 0:
        raise WarehouseError('INVALID_GENERATION')
    store = AggregateStore(writer_factory)
    event_id = uuid4()
    attempted_event_id = event_id
    try:
        with store.connect() as conn, conn.transaction():
            pointer = current(conn, lock=True)
            record = get_run(conn, run_id, lock=True)
            source = check_loaded(conn, record)
            if pointer['run_id'] == run_id:
                # 回执重试无需创建事件或提升 generation。
                event = conn.execute('SELECT * FROM wh_meta.publication_event WHERE event_id=%s',
                                     (pointer['event_id'],)).fetchone()
                attempted_event_id = event['event_id']
            else:
                if expected_generation is not None and pointer['generation'] != expected_generation:
                    raise WarehouseError('PUBLICATION_CONFLICT')
                if pointer['run_id'] and not allow_older:
                    previous = conn.execute('''SELECT b.snapshot_recorded_at FROM wh_meta.aggregate_run a
                        JOIN wh_meta.extract_batch b ON b.source_batch_id=a.source_batch_id
                        WHERE a.run_id=%s''', (pointer['run_id'],)).fetchone()
                    if source['snapshot_recorded_at'] < previous['snapshot_recorded_at']:
                        raise WarehouseError('OLDER_SNAPSHOT_REQUIRES_OVERRIDE')
                event = conn.execute('''INSERT INTO wh_meta.publication_event
                    (event_id,generation,run_id,previous_run_id,allow_older) VALUES (%s,%s,%s,%s,%s) RETURNING *''',
                    (event_id, pointer['generation'] + 1, run_id, pointer['run_id'], allow_older)).fetchone()
                conn.execute('''UPDATE wh_meta.publication SET generation=%s,event_id=%s
                    WHERE name='session_sales_day' ''', (event['generation'], event_id))
            _complete_pipeline(conn, pipeline_id, run_id, event['event_id'])
        return _result(event, record)
    except (Exception, KeyboardInterrupt) as error:
        # COMMIT 成功但客户端丢失回执时，以本次事件和流水线记录确认结果。
        try:
            with store.connect() as conn:
                event = conn.execute('SELECT * FROM wh_meta.publication_event WHERE event_id=%s AND run_id=%s',
                                     (attempted_event_id, run_id)).fetchone()
                completed = pipeline_id is None or conn.execute('''SELECT 1 FROM wh_meta.pipeline_run
                    WHERE run_id=%s AND status='SUCCEEDED' AND publication_event_id=%s''',
                    (pipeline_id, attempted_event_id)).fetchone()
                if event and completed:
                    return _result(event, get_run(conn, run_id))
        except Exception as resolution:
            raise WarehouseError('PUBLICATION_STATUS_UNCERTAIN', aggregate_run_id=run_id,
                                 event_id=attempted_event_id) from resolution
        code = error.code if isinstance(error, WarehouseError) else 'PUBLICATION_FAILED'
        if isinstance(error, KeyboardInterrupt):
            code = 'INTERRUPTED'
        raise WarehouseError(code, aggregate_run_id=run_id) from error
