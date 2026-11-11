"""已验证 ODS → Spark 维度与票级销售；每次生成独立批次，不覆盖来源。"""
from contextlib import suppress
import hashlib
from pathlib import Path
import shutil
from uuid import UUID, uuid4

from pyspark.sql import functions as F

from .batches import BatchStore
from .contracts import SOURCE_TABLES, sales_contract
from .errors import ExtractionError, TransformationError
from .extract import data_directory, verify_batch
from .manifests import sync_directory, write_manifest
from .model_manifests import build_manifest, verify_files
from .model_schemas import MODELS, MODEL_VERSION
from .quality import validate_models, validate_sources
from .spark import create_session
from .transform_runs import TransformStore


ROOT = Path(__file__).resolve().parent
SQL_FILES = {'dim_date': 'dim_date.sql', 'dim_session': 'dim_session.sql', 'dwd_ticket_sale': 'ticket_sale.sql'}


def code_sha256():
    digest = hashlib.sha256()
    for path in sorted(p for p in ROOT.rglob('*') if p.suffix in ('.py', '.sql', '.json')):
        digest.update(str(path.relative_to(ROOT)).encode() + b'\0' + path.read_bytes() + b'\0')
    return digest.hexdigest()


def _result(record):
    return {'status': record['status'], 'run_id': str(record['run_id']),
            'source_batch_id': str(record['source_batch_id']), 'manifest_path': record['manifest_path'],
            'manifest_sha256': record['manifest_sha256'], 'code_sha256': record['code_sha256'],
            'row_counts': record['row_counts'], 'measured_totals': record['measured_totals']}


def verify_transform(run_id, *, data_dir=None, writer_factory=None):
    run_id = UUID(str(run_id))
    record = TransformStore(writer_factory).get(run_id)
    try:
        if record['status'] != 'SUCCEEDED':
            raise TransformationError('TRANSFORM_NOT_SUCCEEDED')
        source = BatchStore(writer_factory).get(record['source_batch_id'])
        if source['status'] != 'SUCCEEDED' or source['manifest_sha256'] != record['input_manifest_sha256']:
            raise TransformationError('SOURCE_BATCH_CHANGED')
        directory = data_directory(data_dir) / 'curated' / str(record['source_batch_id']) / str(run_id)
        verify_files(directory, record)
    except (ExtractionError, TransformationError) as error:
        raise TransformationError(error.code, record['source_batch_id'], run_id) from error
    except Exception as error:
        raise TransformationError('MODEL_VERIFICATION_FAILED', record['source_batch_id'], run_id) from error
    return {**_result(record), 'tables_verified': len(MODELS)}


def _write_outputs(frames, directory):
    for name, frame in frames.items():
        # 首版小数据每表一个 part；清单与校验逻辑支持后续改为多 part。
        frame.coalesce(1).sortWithinPartitions(MODELS[name].primary_key).write.mode('errorifexists').parquet(str(directory / name))


def transform(source_batch_id, *, data_dir=None, writer_factory=None, spark=None):
    source_batch_id = UUID(str(source_batch_id))
    root = data_directory(data_dir)
    try:
        source = verify_batch(source_batch_id, data_dir=root, writer_factory=writer_factory)
    except ExtractionError as error:
        raise TransformationError(error.code, source_batch_id) from error
    source['source_batch_id'] = source_batch_id
    run_id = uuid4()
    fingerprint = code_sha256()
    contract = sales_contract()['version']
    store = TransformStore(writer_factory)
    try:
        store.register(run_id, source, fingerprint, contract, MODEL_VERSION)
    except Exception as error:
        raise TransformationError('TRANSFORM_REGISTRATION_FAILED', source_batch_id, run_id) from error
    staging = root / '.staging' / ('transform-' + str(run_id))
    final = root / 'curated' / str(source_batch_id) / str(run_id)
    owned = False
    owned_spark = spark is None
    session = spark
    cached = []
    try:
        for directory in (staging.parent, final.parent.parent, final.parent):
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            if directory.is_symlink() or not directory.is_dir():
                raise TransformationError('INVALID_MODEL_PATH')
        if final.exists() or final.is_symlink():
            raise TransformationError('MODEL_PATH_EXISTS')
        staging.mkdir(mode=0o700)
        owned = True
        if session is None:
            session = create_session('ticketing-warehouse-sales-transform')
        session.conf.set('spark.sql.session.timeZone', 'UTC')
        session.conf.set('spark.sql.ansi.enabled', 'true')
        session.conf.set('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
        session.conf.set('spark.sql.shuffle.partitions', '2')
        inputs = {}
        for name in SOURCE_TABLES:
            frame = session.read.parquet(str(root / 'ods' / str(source_batch_id) / name / 'part-00000.parquet')).cache()
            cached.append(frame)
            frame.createOrReplaceTempView('ods_' + name)
            inputs[name] = frame
        validate_sources(inputs, source['row_counts'])
        outputs = {}
        for name, sql_file in SQL_FILES.items():
            frame = (session.sql((ROOT / 'sql' / sql_file).read_text())
                     .withColumn('source_batch_id', F.lit(str(source_batch_id)))
                     .withColumn('transform_run_id', F.lit(str(run_id)))
                     .select(*[field for field, _ in MODELS[name].fields]).cache())
            outputs[name] = frame
            cached.append(frame)
        counts, totals = validate_models(outputs, source['reference_totals'], source['row_counts']['activity_session'])
        _write_outputs(outputs, staging)
        # 再核对来源散列，不能把加工期间已变化的 ODS 当作原始批次交付。
        checked = verify_batch(source_batch_id, data_dir=root, writer_factory=writer_factory)
        if checked['manifest_sha256'] != source['manifest_sha256'] or code_sha256() != fingerprint:
            raise TransformationError('SOURCE_OR_CODE_CHANGED')
        lineage = {'run_id': run_id, 'source_batch_id': source_batch_id,
                   'input_manifest_sha256': source['manifest_sha256'], 'code_sha256': fingerprint,
                   'contract_version': contract}
        manifest = build_manifest(staging, lineage, counts, totals, session.version)
        checksum = write_manifest(staging, manifest)
        if final.exists() or final.is_symlink():
            raise TransformationError('MODEL_PATH_EXISTS')
        staging.rename(final)
        owned = False
        sync_directory(final.parent)
        sync_directory(staging.parent)
        record = store.succeed(run_id, f'curated/{source_batch_id}/{run_id}/manifest.json', checksum, counts, totals)
        return _result(record)
    except (Exception, KeyboardInterrupt) as error:
        code = error.code if isinstance(error, (ExtractionError, TransformationError)) else 'TRANSFORMATION_FAILED'
        if isinstance(error, KeyboardInterrupt):
            code = 'INTERRUPTED'
        try:
            record = store.get(run_id)
        except Exception as resolution:
            raise TransformationError('TRANSFORM_STATUS_UNCERTAIN', source_batch_id, run_id) from resolution
        if record['status'] == 'SUCCEEDED':
            return verify_transform(run_id, data_dir=root, writer_factory=writer_factory)
        if record['status'] == 'RUNNING':
            try:
                store.fail(run_id, code)
            except Exception as resolution:
                raise TransformationError('TRANSFORM_STATUS_UNCERTAIN', source_batch_id, run_id) from resolution
        raise TransformationError(code, source_batch_id, run_id) from error
    finally:
        if owned:
            shutil.rmtree(staging, ignore_errors=True)
        if session is not None:
            for frame in cached:
                with suppress(Exception):
                    frame.unpersist()
            for name in SOURCE_TABLES:
                with suppress(Exception):
                    session.catalog.dropTempView('ods_' + name)
            if owned_spark:
                with suppress(Exception):
                    session.stop()
