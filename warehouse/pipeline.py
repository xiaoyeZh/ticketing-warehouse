"""全量运行或从不可变 ODS 重跑；保留各阶段编号，失败后不自动覆盖报表。"""
from contextlib import suppress
from uuid import UUID, uuid4

from .aggregate import aggregate
from .config import connect_role
from .errors import ExtractionError, TransformationError, WarehouseError
from .extract import extract, verify_batch
from .publication import current, publish
from .spark import create_session
from .transform import code_sha256, transform


class PipelineStore:
    def __init__(self, connect=None):
        self.connect = connect or (lambda: connect_role('writer'))

    def register(self, run_id, mode, fingerprint, should_publish):
        with self.connect() as conn:
            generation = current(conn)['generation']
            conn.execute('''INSERT INTO wh_meta.pipeline_run
                (run_id,mode,code_sha256,expected_generation,publish_requested,stage)
                VALUES (%s,%s,%s,%s,%s,%s)''',
                (run_id, mode, fingerprint, generation, should_publish, 'EXTRACT' if mode == 'FULL' else 'TRANSFORM'))
        return generation

    def progress(self, run_id, stage, source_batch_id=None, transform_run_id=None, aggregate_run_id=None):
        with self.connect() as conn:
            row = conn.execute('''UPDATE wh_meta.pipeline_run SET stage=%s,
                source_batch_id=COALESCE(%s,source_batch_id),transform_run_id=COALESCE(%s,transform_run_id),
                aggregate_run_id=COALESCE(%s,aggregate_run_id) WHERE run_id=%s AND status='RUNNING' RETURNING run_id''',
                (stage, source_batch_id, transform_run_id, aggregate_run_id, run_id)).fetchone()
        if not row:
            raise WarehouseError('PIPELINE_STATE_CONFLICT')

    def complete(self, run_id):
        with self.connect() as conn:
            row = conn.execute('''UPDATE wh_meta.pipeline_run SET status='SUCCEEDED',stage='DONE',
                finished_at=clock_timestamp() WHERE run_id=%s AND status='RUNNING' AND NOT publish_requested
                RETURNING run_id''', (run_id,)).fetchone()
        if not row:
            raise WarehouseError('PIPELINE_STATE_CONFLICT')

    def fail(self, run_id, code):
        with self.connect() as conn:
            conn.execute('''UPDATE wh_meta.pipeline_run SET status='FAILED',finished_at=clock_timestamp(),error_code=%s
                WHERE run_id=%s AND status='RUNNING' ''', (code, run_id))

    def get(self, run_id):
        with self.connect() as conn:
            row = conn.execute('SELECT * FROM wh_meta.pipeline_run WHERE run_id=%s', (run_id,)).fetchone()
            if not row:
                raise WarehouseError('PIPELINE_NOT_FOUND', pipeline_run_id=run_id)
            aggregate_row = conn.execute('SELECT measured_totals FROM wh_meta.aggregate_run WHERE run_id=%s',
                                         (row['aggregate_run_id'],)).fetchone() if row['aggregate_run_id'] else None
        return {key: (str(row[key]) if row[key] is not None else None) for key in
                ('run_id', 'source_batch_id', 'transform_run_id', 'aggregate_run_id', 'publication_event_id')} | {
                'status': row['status'], 'mode': row['mode'], 'stage': row['stage'], 'error_code': row['error_code'],
                'publish_requested': row['publish_requested'], 'expected_generation': row['expected_generation'],
                'code_sha256': row['code_sha256'], 'measured_totals': aggregate_row['measured_totals'] if aggregate_row else None}


def run_pipeline(*, source_batch_id=None, should_publish=True, allow_older=False,
                 data_dir=None, reader_factory=None, writer_factory=None, spark=None):
    run_id = uuid4()
    store = PipelineStore(writer_factory)
    fingerprint = code_sha256()
    generation = store.register(run_id, 'REPLAY' if source_batch_id is not None else 'FULL', fingerprint, should_publish)
    session = spark
    try:
        if source_batch_id is None:
            source = extract(data_dir=data_dir, reader_factory=reader_factory, writer_factory=writer_factory)
            source_batch_id = source['source_batch_id']
        else:
            source_batch_id = UUID(str(source_batch_id))
            verify_batch(source_batch_id, data_dir=data_dir, writer_factory=writer_factory)
        store.progress(run_id, 'TRANSFORM', source_batch_id=source_batch_id)
        if session is None:
            session = create_session('ticketing-warehouse-pipeline')
        model = transform(source_batch_id, data_dir=data_dir, writer_factory=writer_factory, spark=session)
        store.progress(run_id, 'AGGREGATE', transform_run_id=UUID(model['run_id']))
        daily = aggregate(model['run_id'], data_dir=data_dir, writer_factory=writer_factory, spark=session)
        store.progress(run_id, 'PUBLISH' if should_publish else 'AGGREGATE', aggregate_run_id=UUID(daily['aggregate_run_id']))
        if code_sha256() != fingerprint:
            raise WarehouseError('SOURCE_OR_CODE_CHANGED')
        if should_publish:
            publish(daily['aggregate_run_id'], expected_generation=generation, allow_older=allow_older,
                    writer_factory=writer_factory, pipeline_id=run_id)
        else:
            store.complete(run_id)
        return store.get(run_id)
    except (Exception, KeyboardInterrupt) as error:
        code = error.code if isinstance(error, (ExtractionError, TransformationError, WarehouseError)) else 'PIPELINE_FAILED'
        if isinstance(error, KeyboardInterrupt):
            code = 'INTERRUPTED'
        try:
            record = store.get(run_id)
            if record['status'] == 'SUCCEEDED':
                return record
            # 下游失败仍保存其编号；外键只引用已经登记的真实任务。
            for key, table in (('source_batch_id', 'extract_batch'), ('transform_run_id', 'transform_run'),
                               ('aggregate_run_id', 'aggregate_run')):
                value = getattr(error, key, None) or getattr(error, 'identifiers', {}).get(key)
                if key == 'transform_run_id' and isinstance(error, TransformationError):
                    value = error.run_id
                if key == 'source_batch_id' and isinstance(error, ExtractionError):
                    value = error.batch_id
                if value:
                    column = 'source_batch_id' if table == 'extract_batch' else 'run_id'
                    with store.connect() as conn:
                        exists = conn.execute(f'SELECT 1 FROM wh_meta.{table} WHERE {column}=%s', (UUID(str(value)),)).fetchone()
                    if exists:
                        store.progress(run_id, record['stage'], **{key: UUID(str(value))})
            store.fail(run_id, code)
        except Exception as resolution:
            raise WarehouseError('PIPELINE_STATUS_UNCERTAIN', pipeline_run_id=run_id) from resolution
        raise WarehouseError(code, pipeline_run_id=run_id) from error
    finally:
        if session is not None and spark is None:
            with suppress(Exception):
                session.stop()
