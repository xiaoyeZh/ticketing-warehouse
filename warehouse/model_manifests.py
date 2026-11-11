"""加工清单校验不启动 Spark；记录并验证全部 part Parquet，而非假定单文件。"""
import hashlib
import json
import os

import pyarrow.parquet as pq

from .errors import TransformationError
from .manifests import sha256_file, sync_directory
from .model_schemas import MODELS, MODEL_VERSION


def _check_file(path, spec):
    with pq.ParquetFile(path) as file:
        # Spark 会推导物理 nullable；逻辑非空约束在提交前由数据质量检查执行。
        if [(f.name, f.type) for f in file.schema_arrow] != list(spec.fields):
            raise TransformationError('MODEL_SCHEMA_MISMATCH')
        return file.metadata.num_rows


def build_manifest(directory, record, counts, totals, spark_version):
    outputs = {}
    for name, spec in MODELS.items():
        files = []
        for path in sorted((directory / name).glob('part-*.parquet')):
            with path.open('rb') as stream:
                os.fsync(stream.fileno())
            files.append({'path': f'{name}/{path.name}', 'size_bytes': path.stat().st_size,
                          'sha256': sha256_file(path), 'row_count': _check_file(path, spec)})
        if not files or sum(entry['row_count'] for entry in files) != counts[name]:
            raise TransformationError('MODEL_ROW_COUNT_MISMATCH')
        sync_directory(directory / name)
        outputs[name] = {'primary_key': spec.primary_key, 'schema': spec.description(),
                         'row_count': counts[name], 'files': files}
    return {'format_version': 1, 'schema_version': MODEL_VERSION, 'run_id': str(record['run_id']),
            'source_batch_id': str(record['source_batch_id']), 'input_manifest_sha256': record['input_manifest_sha256'],
            'contract_version': record['contract_version'], 'code_sha256': record['code_sha256'],
            'spark_version': spark_version, 'outputs': outputs, 'measured_totals': totals}


def verify_files(directory, record):
    expected_path = f"curated/{record['source_batch_id']}/{record['run_id']}/manifest.json"
    if (record['manifest_path'] != expected_path or directory.is_symlink()
            or directory.parent.is_symlink() or directory.parent.parent.is_symlink()):
        raise TransformationError('INVALID_MODEL_PATH')
    path = directory / 'manifest.json'
    if path.is_symlink() or not path.is_file():
        raise TransformationError('MODEL_MANIFEST_MISSING')
    with path.open('rb') as stream:
        data = stream.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024 or hashlib.sha256(data).hexdigest() != record['manifest_sha256']:
        raise TransformationError('MODEL_MANIFEST_CHECKSUM_MISMATCH')
    try:
        manifest = json.loads(data)
        for key in ('run_id', 'source_batch_id', 'input_manifest_sha256', 'contract_version', 'code_sha256'):
            if manifest[key] != str(record[key]):
                raise TransformationError('INVALID_MODEL_MANIFEST')
        if (manifest['format_version'] != 1 or manifest['schema_version'] != MODEL_VERSION
                or record['schema_version'] != MODEL_VERSION or set(manifest['outputs']) != set(MODELS)
                or set(record['row_counts']) != set(MODELS) or manifest['measured_totals'] != record['measured_totals']):
            raise TransformationError('INVALID_MODEL_MANIFEST')
        for name, spec in MODELS.items():
            entry = manifest['outputs'][name]
            if (entry['primary_key'] != spec.primary_key or entry['schema'] != spec.description()
                    or entry['row_count'] != record['row_counts'][name] or not entry['files']
                    or (directory / name).is_symlink()):
                raise TransformationError('INVALID_MODEL_MANIFEST')
            paths = set()
            rows = 0
            for file in entry['files']:
                relative = file['path']
                parts = relative.split('/')
                if (len(parts) != 2 or parts[0] != name or not parts[1].startswith('part-')
                        or not parts[1].endswith('.parquet') or '\\' in relative or relative in paths):
                    raise TransformationError('INVALID_MODEL_PATH')
                paths.add(relative)
                path = directory / relative
                if path.is_symlink() or not path.is_file():
                    raise TransformationError('INVALID_MODEL_PATH')
                if path.stat().st_size != file['size_bytes'] or sha256_file(path) != file['sha256']:
                    raise TransformationError('MODEL_FILE_CHECKSUM_MISMATCH')
                count = _check_file(path, spec)
                if count != file['row_count']:
                    raise TransformationError('MODEL_ROW_COUNT_MISMATCH')
                rows += count
            if rows != entry['row_count']:
                raise TransformationError('MODEL_ROW_COUNT_MISMATCH')
            actual = {f'{name}/{p.name}' for p in (directory / name).glob('*.parquet')}
            if actual != paths:
                raise TransformationError('INVALID_MODEL_MANIFEST')
    except (KeyError, TypeError, ValueError) as error:
        raise TransformationError('INVALID_MODEL_MANIFEST') from error
    return manifest
