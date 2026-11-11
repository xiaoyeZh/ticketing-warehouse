# 第八阶段第六步：完整运行、ODS 重跑与验收

第五步的分步加工和发布见 [日报与发布](08_sales_publication.md)。本步将它们串成可追踪的流水线，并在全新 Docker 环境完成交易与数仓联合验收。

## 1. 一条命令生成当前报表

在现有 `.env`、009 迁移和最新镜像就绪后：

```bash
docker compose --profile warehouse run --rm warehouse run
docker compose --profile warehouse run --rm warehouse-report
```

`run` 顺序执行：记录流水线与当前发布版本 → 七表一致性全量抽取 → 校验 ODS → Spark 维度/票级事实 → Spark 日报与 DWS 装载 → 事务发布。

返回流水线 `run_id`、`source_batch_id`、`transform_run_id`、`aggregate_run_id`、`publication_event_id` 和指标合计。内部共用一个 SparkSession，日常运行不需要初始化管理员密码。

`run` 默认发布整份历史。它是手动批处理入口，本阶段没有安装定时调度器，也不提供增量 CDC 或实时更新。

## 2. 使用原始批次重跑

```bash
docker compose --profile warehouse run --rm warehouse replay --source-batch <source_batch_id>
```

重跑仅验证并读取指定 ODS，生成新的 transform 和 aggregate；即使来源业务表后来改名、退款或出现新订单，也不会混入旧批次。默认只重建和验证，当前报表不切换。需要发布时显式添加 `--publish`：

```bash
docker compose --profile warehouse run --rm warehouse replay --source-batch <source_batch_id> --publish
```

较旧来源需同时显式指定 `--allow-older`，用途和并发保护见第五步。普通重跑与原抽取是同一来源快照，新代码可以产生新的业务结果；只有同一 ODS、同一代码才要求业务结果一致。ODS 文件、旧模型和旧 DWS 版本均不覆盖。

## 3. 状态、失败与恢复

```bash
docker compose --profile warehouse run --rm warehouse pipeline-status --run <pipeline_run_id>
```

`wh_meta.pipeline_run` 保存 FULL/REPLAY、代码散列、开始/结束时间、期望发布版本、阶段、状态、各层批次和可控错误码。阶段为 EXTRACT、TRANSFORM、AGGREGATE、PUBLISH、DONE；下游失败仍保留已登记的失败任务编号。

- 抽取、建模、汇总失败：状态 FAILED，已有成功报表继续可见。检查错误和对应批次，再发起新的 run 或 replay。
- 发布冲突：保留完成的模型和 DWS；先查询最新报表，再决定是否显式 publish 该汇总批次。
- 自动发布时，发布事件、指针和流水线 SUCCEEDED 在同一个数据库事务里提交。事务失败三者一起回滚，避免出现“发布成功但流水线失败”的状态。
- 提交回执丢失时，先查询事件/任务状态确认成功；数据库不可达时返回 `*_STATUS_UNCERTAIN`，不盲目宣称回滚。
- 进程被强制终止可能留下 RUNNING 记录。先用状态查询和批次校验确认情况，再创建独立重跑；当前不会自动把无法确认的 RUNNING 任务改成成功或删除其输出。

本阶段不原地续写失败任务。每次重跑创建新编号，保留失败记录用于定位。文件与数据库不是分布式事务；第四步仍按“成功状态+文件校验”决定能否消费。

## 4. 第六步的验收入口

```bash
python3 scripts/verify_deployment.py --warehouse
```

[验收脚本](../../scripts/verify_deployment.py) 复制源码，生成随机密码、独立 Compose 项目和全新数据卷，执行：

1. 九个迁移与重复初始化、来源/报表权限检查。
2. 原有 Python 测试、所有 ODS/Spark/日报集成测试和无网络 Spark 环境样例。
3. HTTP 登录、支付、出票、退款、重售，以及后台订单过期释放库存。
4. CLI `run` → `replay`，验证七个 ODS 文件、三张模型、两份 DWS 与同源业务散列。
5. 证明默认 replay 不替换报表，显式发布幂等，report 使用 reporter 查询且指标与来源完全一致。
6. 空日期区间、流水线状态查询、ODS 重跑前后散列一致，以及临时容器和数据卷清理。

日志和脱敏摘要默认写入 `artifacts/deployment/`，由 Git 忽略。CI 的 Warehouse job 执行同一个入口，并上传验收产物。

验收完成后，第八阶段的交付是一个可手动执行、可按 ODS 重建、可对账和可原子发布的离线销售数仓。后续若扩展退款净额、核销、调度或增量抽取，应另立指标契约和任务范围。
