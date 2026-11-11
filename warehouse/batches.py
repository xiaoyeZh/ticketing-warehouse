"""writer 独立短连接记录批次，不写入来源只读事务。"""
from psycopg.types.json import Jsonb

from .config import connect_role
from .errors import ExtractionError


class BatchStore:
    def __init__(self, connect=None):
        self.connect = connect or (lambda: connect_role('writer'))

    def register(self, batch_id, contract_version, schema_version):
        with self.connect() as conn:
            conn.execute('''INSERT INTO wh_meta.extract_batch
                (source_batch_id,contract_version,schema_version) VALUES (%s,%s,%s)''',
                         (batch_id, contract_version, schema_version))

    def record_snapshot(self, batch_id, snapshot):
        with self.connect() as conn:
            row = conn.execute('''UPDATE wh_meta.extract_batch
                SET snapshot_recorded_at=%s,source_snapshot=%s,source_database=%s
                WHERE source_batch_id=%s AND status='RUNNING' RETURNING source_batch_id''',
                (snapshot['snapshot_recorded_at'], snapshot['source_snapshot'], snapshot['source_database'], batch_id)).fetchone()
            if not row:
                raise ExtractionError('BATCH_STATE_CONFLICT', batch_id)

    def succeed(self, batch_id, manifest_path, manifest_sha256, row_counts, reference_totals):
        with self.connect() as conn:
            row = conn.execute('''UPDATE wh_meta.extract_batch SET status='SUCCEEDED',
                finished_at=clock_timestamp(),manifest_path=%s,manifest_sha256=%s,row_counts=%s,reference_totals=%s
                WHERE source_batch_id=%s AND status='RUNNING' RETURNING *''',
                (manifest_path, manifest_sha256, Jsonb(row_counts), Jsonb(reference_totals), batch_id)).fetchone()
            if not row:
                raise ExtractionError('BATCH_STATE_CONFLICT', batch_id)
            return row

    def fail(self, batch_id, error_code):
        with self.connect() as conn:
            return conn.execute('''UPDATE wh_meta.extract_batch SET status='FAILED',
                finished_at=clock_timestamp(),error_code=%s
                WHERE source_batch_id=%s AND status='RUNNING' RETURNING *''', (error_code, batch_id)).fetchone()

    def get(self, batch_id):
        with self.connect() as conn:
            row = conn.execute('SELECT * FROM wh_meta.extract_batch WHERE source_batch_id=%s', (batch_id,)).fetchone()
        if not row:
            raise ExtractionError('BATCH_NOT_FOUND', batch_id)
        return row
