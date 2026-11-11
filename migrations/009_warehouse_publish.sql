-- 日报按批次保存；只有事务切换的发布指针可被报表账号读取。
BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE TABLE wh_meta.aggregate_run (
    run_id UUID PRIMARY KEY,
    transform_run_id UUID NOT NULL REFERENCES wh_meta.transform_run(run_id) ON DELETE RESTRICT,
    source_batch_id UUID NOT NULL REFERENCES wh_meta.extract_batch(source_batch_id) ON DELETE RESTRICT,
    input_manifest_sha256 TEXT NOT NULL CHECK (input_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    code_sha256 TEXT NOT NULL CHECK (code_sha256 ~ '^[0-9a-f]{64}$'),
    contract_version TEXT NOT NULL,
    schema_version TEXT NOT NULL CHECK (schema_version='sales-dws-v1'),
    status TEXT NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    row_count BIGINT,
    measured_totals JSONB,
    data_sha256 TEXT,
    error_code TEXT,
    CHECK (
        (status='RUNNING' AND finished_at IS NULL AND error_code IS NULL AND row_count IS NULL
         AND measured_totals IS NULL AND data_sha256 IS NULL) OR
        (status='FAILED' AND finished_at IS NOT NULL AND error_code IS NOT NULL AND row_count IS NULL
         AND measured_totals IS NULL AND data_sha256 IS NULL) OR
        (status='SUCCEEDED' AND finished_at IS NOT NULL AND error_code IS NULL AND row_count >= 0
         AND measured_totals IS NOT NULL AND jsonb_typeof(measured_totals)='object'
         AND data_sha256 IS NOT NULL AND data_sha256 ~ '^[0-9a-f]{64}$')
    )
);
CREATE INDEX ix_aggregate_transform ON wh_meta.aggregate_run(transform_run_id, started_at DESC);

CREATE TABLE dws.session_sales_day (
    aggregate_run_id UUID NOT NULL REFERENCES wh_meta.aggregate_run(run_id) ON DELETE RESTRICT,
    paid_date DATE NOT NULL,
    session_id BIGINT NOT NULL,
    activity_id BIGINT NOT NULL,
    activity_title TEXT NOT NULL,
    venue_id BIGINT NOT NULL,
    venue_name TEXT NOT NULL,
    session_start_at TIMESTAMPTZ NOT NULL,
    sold_ticket_count BIGINT NOT NULL CHECK (sold_ticket_count > 0),
    paid_order_count BIGINT NOT NULL CHECK (paid_order_count > 0 AND paid_order_count <= sold_ticket_count),
    paid_amount NUMERIC(28,2) NOT NULL CHECK (paid_amount >= 0),
    PRIMARY KEY (aggregate_run_id, paid_date, session_id)
);

CREATE TABLE wh_meta.publication_event (
    event_id UUID PRIMARY KEY,
    generation BIGINT NOT NULL UNIQUE CHECK (generation > 0),
    run_id UUID NOT NULL REFERENCES wh_meta.aggregate_run(run_id) ON DELETE RESTRICT,
    previous_run_id UUID REFERENCES wh_meta.aggregate_run(run_id) ON DELETE RESTRICT,
    published_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    allow_older BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE wh_meta.publication (
    name TEXT PRIMARY KEY CHECK (name='session_sales_day'),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    event_id UUID REFERENCES wh_meta.publication_event(event_id) ON DELETE RESTRICT,
    CHECK ((generation=0 AND event_id IS NULL) OR (generation>0 AND event_id IS NOT NULL))
);
INSERT INTO wh_meta.publication(name) VALUES ('session_sales_day');

CREATE TABLE wh_meta.pipeline_run (
    run_id UUID PRIMARY KEY,
    mode TEXT NOT NULL CHECK (mode IN ('FULL','REPLAY')),
    code_sha256 TEXT NOT NULL CHECK (code_sha256 ~ '^[0-9a-f]{64}$'),
    expected_generation BIGINT NOT NULL CHECK (expected_generation >= 0),
    publish_requested BOOLEAN NOT NULL,
    stage TEXT NOT NULL CHECK (stage IN ('EXTRACT','TRANSFORM','AGGREGATE','PUBLISH','DONE')),
    source_batch_id UUID REFERENCES wh_meta.extract_batch(source_batch_id) ON DELETE RESTRICT,
    transform_run_id UUID REFERENCES wh_meta.transform_run(run_id) ON DELETE RESTRICT,
    aggregate_run_id UUID REFERENCES wh_meta.aggregate_run(run_id) ON DELETE RESTRICT,
    publication_event_id UUID REFERENCES wh_meta.publication_event(event_id) ON DELETE RESTRICT,
    status TEXT NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    error_code TEXT,
    CHECK (
        (status='RUNNING' AND finished_at IS NULL AND error_code IS NULL) OR
        (status='FAILED' AND finished_at IS NOT NULL AND error_code IS NOT NULL) OR
        (status='SUCCEEDED' AND finished_at IS NOT NULL AND error_code IS NULL AND stage='DONE'
         AND source_batch_id IS NOT NULL AND transform_run_id IS NOT NULL AND aggregate_run_id IS NOT NULL
         AND (NOT publish_requested OR publication_event_id IS NOT NULL))
    )
);

-- 普通视图使用创建者权限访问底层对象；reporter 仅获得 ads 的 SELECT。
CREATE VIEW ads.session_sales_publication AS
SELECT p.generation, e.event_id, e.published_at, a.run_id AS aggregate_run_id,
       a.transform_run_id, a.source_batch_id, a.contract_version, b.snapshot_recorded_at,
       a.row_count, a.measured_totals
FROM wh_meta.publication p
LEFT JOIN wh_meta.publication_event e ON e.event_id=p.event_id AND e.generation=p.generation
LEFT JOIN wh_meta.aggregate_run a ON a.run_id=e.run_id AND a.status='SUCCEEDED'
LEFT JOIN wh_meta.extract_batch b ON b.source_batch_id=a.source_batch_id
WHERE p.name='session_sales_day';

CREATE VIEW ads.session_sales_day AS
SELECT s.*, p.generation, p.source_batch_id, p.transform_run_id, p.snapshot_recorded_at,
       p.published_at, p.contract_version
FROM ads.session_sales_publication p
JOIN dws.session_sales_day s ON s.aggregate_run_id=p.aggregate_run_id;

COMMENT ON TABLE dws.session_sales_day IS 'sales-v1：上海支付日期×场次；退款保留原销售，场次名称来自同一建模快照';
COMMENT ON TABLE wh_meta.publication IS '全历史替换；行锁与 generation 防止并发任务覆盖已发布版本';
COMMIT;
