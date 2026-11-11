# 第八阶段第五步：每日场次汇总与报表发布

本步将第四步的票级销售 Parquet 交给 Spark SQL，按上海支付日期×场次生成日报，装载 PostgreSQL DWS，再通过 ADS 发布查询。运行入口为 `aggregate`、`verify-aggregate`、`publish`、`report`；整条流水线见 [第六步](08_pipeline.md)。

## 1. 初始化和分步运行

沿用仓库根目录已有的 `.env`。先备份数据库，再构建应用与数仓镜像、应用新增 009 迁移并确认三类账号授权：

```bash
docker compose build db-admin
docker compose --profile warehouse build warehouse warehouse-tests
docker compose --profile warehouse run --rm warehouse-admin
```

`warehouse-admin` 只用于迁移与授权，普通加工和查询使用受限账号。已有迁移不改写；009 只新增分析对象，不更新票务业务记录。

已有第四步成功建模批次时，可以分步运行；下列尖括号需替换为实际返回的 UUID：

```bash
docker compose --profile warehouse run --rm warehouse aggregate --transform-run <transform_run_id>
docker compose --profile warehouse run --rm warehouse verify-aggregate --run <aggregate_run_id>
docker compose --profile warehouse run --rm warehouse publish --run <aggregate_run_id>
docker compose --profile warehouse run --rm warehouse-report
docker compose --profile warehouse run --rm warehouse-report report --start-date 2026-11-01 --end-date 2026-12-01
```

日期左闭右开，按支付日期筛选；`--session <session_id>` 可同时限制场次。没有发布过数据时返回 `REPORT_NOT_PUBLISHED`；发布成功的空批次、无销售场次或空日期区间返回空列表和 0、0、0.00 合计。

## 2. Spark 如何生成日报

[session_sales_day.sql](../../warehouse/sql/session_sales_day.sql) 对已验证的 `dwd_ticket_sale` 分组：

| 字段 | 计算 |
|---|---|
| paid_date、session_id | 上海支付日期×场次，沿用 DWD 已计算的支付日期 |
| sold_ticket_count | COUNT(*)，销售事实已按 ticket_id 唯一 |
| paid_order_count | COUNT(DISTINCT order_id) |
| paid_amount | SUM(sale_amount)，显式 DECIMAL(28,2) |

关联同一 transform 的 `dim_session`，附带活动和场馆编号/名称、场次开始时间。不会回查实时名称，也不向日报暴露购票用户编号。退款保留原销售；零金额票仍计票数和订单数；无销售日期/场次不造行。

[aggregate.py](../../warehouse/aggregate.py) 加工前后验证建模清单、文件散列、行数、必填字段、唯一键、关联、行级批次、来源合计与代码散列。额外检查每个订单只属于一个日期/场次，因此日报订单数可以跨组相加。金额始终用 Decimal，禁止 float。

首版在本地 Spark 执行全历史重建，只把聚合后的日报组物化到 Python driver 中再加载；票级明细不 collect 到 Python。日报组数增大后应改为分块暂存与装载，不把当前实现当作无内存上限的分布式写库方案。

## 3. DWS、校验和可追溯性

[009 迁移](../../migrations/009_warehouse_publish.sql) 新增：

- `wh_meta.aggregate_run`：输入 transform/ODS 编号、输入清单散列、代码散列、契约/结构版本、状态、行数、合计、内容 SHA-256。
- `dws.session_sales_day`：主键为 `(aggregate_run_id, paid_date, session_id)`，每次重建保留独立版本。
- `wh_meta.publication` 与 `publication_event`：当前发布指针及所有切换事件。
- `wh_meta.pipeline_run`：第六步的执行阶段、各层编号和发布事件。
- `ads.session_sales_publication` 与 `ads.session_sales_day`：发布元信息与当前日报视图。

[aggregate_runs.py](../../warehouse/aggregate_runs.py) 在一个数据库事务里插入整批 DWS，按确定顺序复核行数、金额与内容散列，最后登记 SUCCEEDED。中间失败整批回滚，再单独登记 FAILED。数据库成功但客户端丢失回执时，重新检查成功状态与数据，不降级或重复插入。

`verify-aggregate` 在同一个只读 REPEATABLE READ 快照内核对数据库中的日报、输入批次血缘、来源/DWD 合计与内容散列。DWS 内容散列排除运行编号，包含日期、场次、名称和三个指标；同一 ODS、相同代码的重跑应得到相同业务散列。

## 4. 发布为何不会出现半份数据

[publication.py](../../warehouse/publication.py) 锁定唯一发布记录，在同一事务内复核完整 DWS、写入发布事件、切换指针。ADS 视图只读取指针所指向的成功批次。失败时整个切换回滚，旧报表保持可见；重复发布当前批次不新增事件、不提升 generation。

`--expected-generation N` 是比较后切换：并发期间已有其他任务发布，则返回 `PUBLICATION_CONFLICT`，保留两份加工结果供检查。完整 `run` 在启动时记录 generation，并自动使用此保护。

默认不允许旧来源快照覆盖较新的已发布来源。明确需要恢复旧版本时：

```bash
docker compose --profile warehouse run --rm warehouse-report
docker compose --profile warehouse run --rm warehouse publish --run <old_aggregate_run_id> --expected-generation <current_generation> --allow-older
```

检查当前报表的 generation 后再替换参数。此操作创建新的发布事件，保留旧事件，不删除任何历史数据。同源快照重新加工的批次仍可正常发布。

## 5. 报表账号与一致性

`warehouse` 服务仅接收 reader/writer 密码；独立的 `warehouse-report` 服务只接收 reporter 密码，不挂载 ODS 数据卷。reporter 只能 SELECT ADS，不能访问 public 业务表、wh_meta 或 DWS，也不能写报表。

[report.py](../../warehouse/report.py) 在一个只读 REPEATABLE READ 事务里查询发布信息、日报行与筛选合计。因此即使查询途中有其他任务发布，也不会把旧批次编号和新批次数据拼在一起。输出附有来源快照时间；快照表示抽取时可见的全部历史，不是历史日期的日终库存或实时销售状态。

## 6. 验证

```bash
docker compose --profile warehouse run --rm --build warehouse-tests
python3 scripts/verify_deployment.py --warehouse
```

[发布集成测试](../../tests/warehouse/test_publish.py) 使用真实 Spark、随机账号和隔离数据库，覆盖每日多场次分组、退款重售、零金额、上海午夜、空数据、重跑、装载回滚、发布回滚、回执丢失、内容损坏、并发发布、恢复旧快照、查询期间切换版本以及 reporter 权限。完整验收还回归原票务系统，不对开发库执行测试清理。
