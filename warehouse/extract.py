"""七张业务表的一致性全量抽取；数据库成功状态与文件校验共同构成可消费条件。"""
import os
from pathlib import Path
import shutil
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import sql

from .batches import BatchStore
from .config import connect_role
from .contracts import SOURCE_TABLES, sales_contract
from .errors import ExtractionError
from .manifests import build_manifest, sync_directory, verify_files, write_manifest
from .schemas import SCHEMA_VERSION, TABLE_SPECS


def data_directory(directory=None):
    return Path(directory or os.environ.get('WAREHOUSE_DATA_DIR', '/warehouse-data')).resolve()


def _result(record):
    return {'status': record['status'], 'source_batch_id': str(record['source_batch_id']),
            'manifest_path': record['manifest_path'], 'manifest_sha256': record['manifest_sha256'],
            'row_counts': record['row_counts'], 'reference_totals': record['reference_totals']}


def verify_batch(batch_id, *, data_dir=None, writer_factory=None):
    batch_id = UUID(str(batch_id))
    record = BatchStore(writer_factory).get(batch_id)
    if record['status'] != 'SUCCEEDED':
        raise ExtractionError('BATCH_NOT_SUCCEEDED', batch_id)
    try:
        verify_files(data_directory(data_dir) / 'ods' / str(batch_id), record)
    except ExtractionError as exc:
        if exc.batch_id is None:
            raise ExtractionError(exc.code, batch_id) from exc
        raise
    except Exception as exc:
        raise ExtractionError('BATCH_VERIFICATION_FAILED', batch_id) from exc
    return {**_result(record), 'files_verified': len(SOURCE_TABLES)}


def _extract_table(reader, spec, directory, chunk_size):
    spec.validate_source(reader)
    expected = reader.execute(sql.SQL('SELECT count(*) AS value FROM public.{}').format(
        sql.Identifier(spec.name))).fetchone()['value']
    target = directory / spec.name
    target.mkdir(mode=0o700)
    written = 0
    with reader.cursor(name='ods_' + spec.name) as cursor:
        cursor.execute(spec.query)
        with pq.ParquetWriter(target / 'part-00000.parquet', spec.arrow_schema, compression='snappy') as output:
            while rows := cursor.fetchmany(chunk_size):
                table = pa.Table.from_pylist(spec.normalize(rows), schema=spec.arrow_schema)
                table.validate(full=True)
                output.write_table(table)
                written += len(rows)
            if not written:
                output.write_table(pa.Table.from_pylist([], schema=spec.arrow_schema))
    if written != expected:
        raise ExtractionError('ROW_COUNT_MISMATCH')
    return written


def _reference_totals(reader):
    paid = reader.execute('''SELECT count(*) AS paid_order_count,
        COALESCE(sum(p.amount),0::numeric) AS paid_amount
        FROM payment_attempt p JOIN booking_order o ON o.order_id=p.order_id
        WHERE p.status='SUCCEEDED' AND o.paid_at IS NOT NULL''').fetchone()
    tickets = reader.execute('''SELECT count(*) AS sold_ticket_count FROM ticket t
        JOIN booking_order o ON o.order_id=t.order_id
        JOIN payment_attempt p ON p.order_id=o.order_id
        WHERE p.status='SUCCEEDED' AND o.paid_at IS NOT NULL''').fetchone()
    return {'paid_order_count': paid['paid_order_count'], 'paid_amount': format(paid['paid_amount'], '.2f'),
            'sold_ticket_count': tickets['sold_ticket_count']}


def extract(*, data_dir=None, chunk_size=5000, reader_factory=None, writer_factory=None):
    if type(chunk_size) is not int or not 1 <= chunk_size <= 100000:
        raise ExtractionError('INVALID_CHUNK_SIZE')
    root = data_directory(data_dir)
    root.mkdir(parents=True, exist_ok=True)
    for name in ('.staging', 'ods'):
        base = root / name
        base.mkdir(mode=0o700, exist_ok=True)
        if base.is_symlink() or not base.is_dir():
            raise ExtractionError('INVALID_BATCH_PATH')
    batch_id = uuid4()
    store = BatchStore(writer_factory)
    try:
        store.register(batch_id, sales_contract()['version'], SCHEMA_VERSION)
    except Exception as exc:
        # 注册回执可能丢失，也可能是 UUID 冲突，不能更改已有批次。
        raise ExtractionError('BATCH_REGISTRATION_FAILED', batch_id) from exc
    staging = root / '.staging' / str(batch_id)
    final = root / 'ods' / str(batch_id)
    owns_staging = False
    try:
        if final.exists() or final.is_symlink():
            raise ExtractionError('BATCH_PATH_EXISTS', batch_id)
        staging.mkdir(mode=0o700)
        owns_staging = True
        factory = reader_factory or (lambda: connect_role('reader'))
        with factory() as reader, reader.transaction():
            reader.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            reader.execute("SET LOCAL statement_timeout='5min'")
            snapshot = reader.execute('''SELECT pg_current_snapshot()::text AS source_snapshot,
                clock_timestamp() AS snapshot_recorded_at,current_database() AS source_database''').fetchone()
            store.record_snapshot(batch_id, snapshot)
            counts = {name: _extract_table(reader, TABLE_SPECS[name], staging, chunk_size) for name in SOURCE_TABLES}
            reference = _reference_totals(reader)
        # 校验与散列在释放来源快照后执行，缩短只读事务持续时间。
        manifest = build_manifest(staging, batch_id, snapshot, counts, reference)
        checksum = write_manifest(staging, manifest)
        if final.exists() or final.is_symlink():
            raise ExtractionError('BATCH_PATH_EXISTS', batch_id)
        staging.rename(final)
        owns_staging = False
        sync_directory(final.parent)
        sync_directory(staging.parent)
        record = store.succeed(batch_id, f'ods/{batch_id}/manifest.json', checksum, counts, reference)
        return _result(record)
    except (Exception, KeyboardInterrupt) as exc:
        # 先核对状态：成功提交的回执丢失时不允许降级或删除已交付文件。
        try:
            record = store.get(batch_id)
            if record['status'] == 'SUCCEEDED':
                return verify_batch(batch_id, data_dir=root, writer_factory=writer_factory)
            if record['status'] == 'RUNNING':
                code = exc.code if isinstance(exc, ExtractionError) else 'EXTRACTION_FAILED'
                if isinstance(exc, KeyboardInterrupt):
                    code = 'INTERRUPTED'
                store.fail(batch_id, code)
        except Exception as resolution:
            raise ExtractionError('BATCH_STATUS_UNCERTAIN', batch_id) from resolution
        finally:
            if owns_staging:
                shutil.rmtree(staging, ignore_errors=True)
        code = exc.code if isinstance(exc, ExtractionError) else 'EXTRACTION_FAILED'
        raise ExtractionError(code, batch_id) from exc
