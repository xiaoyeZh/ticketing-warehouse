"""writer 为每次加工保存输入清单、代码散列和独立输出批次。"""
from psycopg.types.json import Jsonb

from .config import connect_role
from .errors import TransformationError


class TransformStore:
    def __init__(self, connect=None):
        self.connect = connect or (lambda: connect_role('writer'))

    def register(self, run_id, source, code_sha256, contract_version, schema_version):
        with self.connect() as conn:
            row = conn.execute('''INSERT INTO wh_meta.transform_run
                (run_id,source_batch_id,input_manifest_sha256,code_sha256,contract_version,schema_version)
                SELECT %s,source_batch_id,manifest_sha256,%s,%s,%s FROM wh_meta.extract_batch
                WHERE source_batch_id=%s AND status='SUCCEEDED' AND manifest_sha256=%s RETURNING run_id''',
                (run_id, code_sha256, contract_version, schema_version,
                 source['source_batch_id'], source['manifest_sha256'])).fetchone()
        if not row:
            raise TransformationError('SOURCE_BATCH_CHANGED', source['source_batch_id'], run_id)

    def succeed(self, run_id, path, checksum, counts, totals):
        with self.connect() as conn:
            row = conn.execute('''UPDATE wh_meta.transform_run SET status='SUCCEEDED',
                finished_at=clock_timestamp(),manifest_path=%s,manifest_sha256=%s,row_counts=%s,measured_totals=%s
                WHERE run_id=%s AND status='RUNNING' RETURNING *''',
                (path, checksum, Jsonb(counts), Jsonb(totals), run_id)).fetchone()
        if not row:
            raise TransformationError('TRANSFORM_STATE_CONFLICT', run_id=run_id)
        return row

    def fail(self, run_id, code):
        with self.connect() as conn:
            conn.execute('''UPDATE wh_meta.transform_run SET status='FAILED',finished_at=clock_timestamp(),error_code=%s
                WHERE run_id=%s AND status='RUNNING' ''', (code, run_id))

    def get(self, run_id):
        with self.connect() as conn:
            row = conn.execute('SELECT * FROM wh_meta.transform_run WHERE run_id=%s', (run_id,)).fetchone()
        if not row:
            raise TransformationError('TRANSFORM_NOT_FOUND', run_id=run_id)
        return row
