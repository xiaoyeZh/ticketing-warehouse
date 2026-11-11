"""ODS 文件校验与清单；只接受本版七张表的固定相对路径。"""
import hashlib
import json
import os
from pathlib import Path

import pyarrow.parquet as pq

from .contracts import SOURCE_TABLES, sales_contract
from .errors import ExtractionError
from .schemas import SCHEMA_VERSION, TABLE_SPECS


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def check_parquet(path, spec, expected_rows):
    parquet = pq.ParquetFile(path)
    try:
        if not parquet.schema_arrow.equals(spec.arrow_schema, check_metadata=False):
            raise ExtractionError('PARQUET_SCHEMA_MISMATCH')
        if parquet.metadata.num_rows != expected_rows:
            raise ExtractionError('ROW_COUNT_MISMATCH')
    finally:
        parquet.close()


def build_manifest(directory, batch_id, snapshot, table_counts, reference_totals):
    tables = {}
    for name in SOURCE_TABLES:
        spec = TABLE_SPECS[name]
        relative = f'{name}/part-00000.parquet'
        path = directory / relative
        check_parquet(path, spec, table_counts[name])
        with path.open('rb') as stream:
            os.fsync(stream.fileno())
        sync_directory(path.parent)
        tables[name] = {'path': relative, 'row_count': table_counts[name],
                        'source_row_count': table_counts[name], 'size_bytes': path.stat().st_size,
                        'sha256': sha256_file(path), 'primary_key': spec.primary_key, 'schema': spec.description()}
    return {'format_version': 1, 'extractor_version': '1', 'source_batch_id': str(batch_id),
            'contract_version': sales_contract()['version'], 'schema_version': SCHEMA_VERSION,
            'snapshot': {**snapshot, 'snapshot_recorded_at': snapshot['snapshot_recorded_at'].isoformat()},
            'tables': tables, 'reference_totals': reference_totals}


def write_manifest(directory, manifest):
    path = directory / 'manifest.json'
    with path.open('x', encoding='utf-8') as stream:
        json.dump(manifest, stream, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    sync_directory(directory)
    return sha256_file(path)


def verify_files(directory, record):
    batch_id = record['source_batch_id']
    expected_path = f'ods/{batch_id}/manifest.json'
    if record['manifest_path'] != expected_path or directory.is_symlink() or directory.parent.is_symlink():
        raise ExtractionError('INVALID_BATCH_PATH', batch_id)
    path = directory / 'manifest.json'
    if path.is_symlink() or not path.is_file():
        raise ExtractionError('MANIFEST_MISSING', batch_id)
    with path.open('rb') as stream:
        data = stream.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ExtractionError('INVALID_MANIFEST', batch_id)
    # 对实际解析的同一份字节计算散列，不在散列后重新打开清单读取。
    if hashlib.sha256(data).hexdigest() != record['manifest_sha256']:
        raise ExtractionError('MANIFEST_CHECKSUM_MISMATCH', batch_id)
    try:
        manifest = json.loads(data)
        if (manifest['format_version'] != 1 or manifest['source_batch_id'] != str(batch_id)
                or manifest['contract_version'] != sales_contract()['version']
                or manifest['schema_version'] != SCHEMA_VERSION
                or record['contract_version'] != manifest['contract_version']
                or record['schema_version'] != manifest['schema_version']
                or manifest['reference_totals'] != record['reference_totals']
                or manifest['snapshot']['source_snapshot'] != record['source_snapshot']
                or manifest['snapshot']['source_database'] != record['source_database']
                or manifest['snapshot']['snapshot_recorded_at'] != record['snapshot_recorded_at'].isoformat()
                or set(manifest['tables']) != set(SOURCE_TABLES)
                or set(record['row_counts']) != set(SOURCE_TABLES)):
            raise ExtractionError('INVALID_MANIFEST', batch_id)
        for name in SOURCE_TABLES:
            spec, entry = TABLE_SPECS[name], manifest['tables'][name]
            relative = f'{name}/part-00000.parquet'
            if (entry['path'] != relative or entry['primary_key'] != spec.primary_key
                    or entry['schema'] != spec.description() or type(entry['row_count']) is not int
                    or entry['row_count'] < 0 or entry['source_row_count'] != entry['row_count']
                    or record['row_counts'][name] != entry['row_count']):
                raise ExtractionError('INVALID_MANIFEST', batch_id)
            file = directory / relative
            if file.parent.is_symlink() or file.is_symlink() or not file.is_file():
                raise ExtractionError('INVALID_BATCH_PATH', batch_id)
            if file.stat().st_size != entry['size_bytes'] or sha256_file(file) != entry['sha256']:
                raise ExtractionError('FILE_CHECKSUM_MISMATCH', batch_id)
            check_parquet(file, spec, entry['row_count'])
    except (KeyError, TypeError, ValueError) as exc:
        raise ExtractionError('INVALID_MANIFEST', batch_id) from exc
    return manifest
