-- 第八阶段前两步：只建立数仓命名空间；指标表在后续加工步骤新增迁移。
BEGIN;
SET LOCAL search_path = public;
SET LOCAL lock_timeout = '5s';

CREATE SCHEMA wh_meta;
CREATE SCHEMA dws;
CREATE SCHEMA ads;
REVOKE ALL ON SCHEMA wh_meta, dws, ads FROM PUBLIC;
COMMENT ON SCHEMA wh_meta IS '数仓抽取批次、任务状态与发布元数据';
COMMENT ON SCHEMA dws IS '数仓按批次保存的汇总数据';
COMMENT ON SCHEMA ads IS '仅展示已发布批次的报表视图';

COMMIT;
