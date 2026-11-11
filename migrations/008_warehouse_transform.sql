-- Spark 维度/销售明细加工批次；汇总和报表发布在后续阶段实现。
BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE TABLE wh_meta.transform_run (
    run_id UUID PRIMARY KEY,
    source_batch_id UUID NOT NULL REFERENCES wh_meta.extract_batch (source_batch_id) ON DELETE RESTRICT,
    input_manifest_sha256 TEXT NOT NULL CHECK (input_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    code_sha256 TEXT NOT NULL CHECK (code_sha256 ~ '^[0-9a-f]{64}$'),
    contract_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    manifest_path TEXT,
    manifest_sha256 TEXT,
    row_counts JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(row_counts)='object'),
    measured_totals JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(measured_totals)='object'),
    error_code TEXT,
    CONSTRAINT ck_transform_status CHECK (
        (status='RUNNING' AND finished_at IS NULL AND error_code IS NULL
         AND manifest_path IS NULL AND manifest_sha256 IS NULL) OR
        (status='FAILED' AND finished_at IS NOT NULL AND error_code IS NOT NULL
         AND manifest_path IS NULL AND manifest_sha256 IS NULL) OR
        (status='SUCCEEDED' AND finished_at IS NOT NULL AND error_code IS NULL
         AND manifest_path IS NOT NULL AND manifest_sha256 IS NOT NULL
         AND manifest_sha256 ~ '^[0-9a-f]{64}$')
    )
);
CREATE INDEX ix_transform_source ON wh_meta.transform_run (source_batch_id, started_at DESC);
COMMENT ON TABLE wh_meta.transform_run IS '同一 ODS 可生成多个独立加工批次；仅成功且文件校验通过的结果可消费';

COMMIT;
