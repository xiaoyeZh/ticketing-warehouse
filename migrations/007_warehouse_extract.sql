-- ODS 原始批次登记。文件系统与数据库分别提交，消费者须同时验证状态与文件。
BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE TABLE wh_meta.extract_batch (
    source_batch_id UUID PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    contract_version TEXT NOT NULL CHECK (length(btrim(contract_version)) > 0),
    schema_version TEXT NOT NULL CHECK (length(btrim(schema_version)) > 0),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    snapshot_recorded_at TIMESTAMPTZ,
    source_snapshot TEXT,
    source_database TEXT,
    finished_at TIMESTAMPTZ,
    manifest_path TEXT,
    manifest_sha256 TEXT,
    row_counts JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(row_counts)='object'),
    reference_totals JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(reference_totals)='object'),
    error_code TEXT,
    CONSTRAINT ck_extract_snapshot_shape CHECK (
        (snapshot_recorded_at IS NULL AND source_snapshot IS NULL AND source_database IS NULL) OR
        (snapshot_recorded_at IS NOT NULL AND source_snapshot IS NOT NULL AND source_database IS NOT NULL)
    ),
    CONSTRAINT ck_extract_status_shape CHECK (
        (status='RUNNING' AND finished_at IS NULL AND error_code IS NULL
         AND manifest_path IS NULL AND manifest_sha256 IS NULL) OR
        (status='FAILED' AND finished_at IS NOT NULL AND error_code IS NOT NULL
         AND manifest_path IS NULL AND manifest_sha256 IS NULL) OR
        (status='SUCCEEDED' AND finished_at IS NOT NULL AND error_code IS NULL
         AND source_snapshot IS NOT NULL AND manifest_path IS NOT NULL
         AND manifest_sha256 IS NOT NULL AND manifest_sha256 ~ '^[0-9a-f]{64}$')
    )
);
CREATE INDEX ix_extract_batch_started ON wh_meta.extract_batch (started_at DESC);
COMMENT ON TABLE wh_meta.extract_batch IS '全量一致性抽取批次；仅 SUCCEEDED 且清单/文件验证通过时可消费';

COMMIT;
