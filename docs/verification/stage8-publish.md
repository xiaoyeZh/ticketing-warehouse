# 第八阶段第五、六步验证：日报发布与完整流水线

日期：2026-11-11（Asia/Shanghai）；本地平台：macOS / Apple Silicon ARM64。
分支：codex/offline-warehouse；本步起点：7b0e699。

## 隔离环境验收

执行 `python3 scripts/verify_deployment.py --warehouse`。
临时项目：ticketing-verify-5d7b958b99964414；迁移数：9；18:16～18:19 验收通过。

- 原交易与权限测试 62 项通过。
- 数仓测试 52 项通过：ODS 18 项、建模 15 项、日报发布与流水线 19 项。
- 合计 114 项；[测试摘要](stage8-publish-tests.txt)。完整日志保存在本地被 Git 忽略的 `artifacts/warehouse-publish/`。
- 离线 Spark 环境、CLI 完整 run、同源 replay、七表 ODS/三表模型/DWS 校验、显式发布、发布幂等、只读 report、空区间和 pipeline-status 均通过。
- 来源快照合计、DWD、DWS、ADS 报表合计一致，同一 ODS 重跑的日报业务散列一致。
- HTTP 支付、退款、重售与后台过期释放通过；临时容器、数据卷和测试数据库清理通过。

19 项新增集成用例覆盖日期×多场次分组、整单退款重售、零金额与上海跨日、未发布/空批次区分、重复汇总/发布、装载回滚、发布回滚、提交回执丢失、DWS 损坏、真实双连接并发发布、旧快照恢复、报表查询期间发布、reporter 权限、完整运行、默认不发布的 ODS 重跑、失败阶段追踪和流水线完成回执丢失。

首轮测试中，联接视图 DELETE 被 PostgreSQL 先以“视图不可更新”拒绝，权限测试对错误类型的预期过窄。测试已改为核验 reporter 对 ADS 仅有 SELECT 权限，并保留对 public、wh_meta、DWS 直接访问的拒绝断言；最终完整验收全部通过。
原始日志包含 Spark/Python ResourceWarning 诊断；测试退出码为 0，全部断言通过，摘要省略重复诊断。
本地结果不替代远端平台验证，Linux 结果以本分支对应提交的 GitHub Actions 为准。

## 开发库实际运行

迁移前备份已通过 pg_restore --list 检查：
`backups/pre-warehouse-publish-20261111-181918.dump`，85780 字节，不进入 Git 或镜像。
管理员入口仅应用新增的 009 迁移，未改写原迁移或票务业务记录。

| 对象 | 编号 |
|---|---|
| 完整流水线 | e57c518d-a981-4c2d-bf1b-cb95c0b5f6bd |
| ODS 来源 | 78bc130d-89df-4c2e-a251-3b8a18eac67d |
| Spark 模型 | dbda8dcd-ebd3-4cf9-a09e-815c900b6b68 |
| DWS 汇总 | 7c0290a5-7f09-4df6-b3b4-f1a3ff44d4e7 |
| 发布事件 | aa72c2b8-61d4-4861-9169-61b2eb797475 |
| 验证重跑流水线 | 75cc449b-e719-42fe-9497-5779ad4afed5 |
| 重跑 DWS | 9be4c5ab-32a3-4d10-9148-74e4425f2cd6 |

代码 SHA-256：`14c6f3dcc98940e0a9587164afabf1e7e981a38d40b223c05808750a7da45f46`。
两份日报的业务内容 SHA-256 均为 `94c80cff6670fb00e6eb05bd7b65318f8f2b0ad83d08b58a56979e28b31b9f48`。

报表当前 generation=1，一行对应上海支付日期 2026-11-02、session_id=13：历史成功售票 14 张、成功支付订单 7 单、模拟支付金额 1540.00。退款后的原销售继续保留，不代表退款净额或当前库存。
完整流水线和默认不发布的重跑均为 SUCCEEDED / DONE，重跑前后报表完全一致。

原第三步 ODS `cc90492f-55bc-48aa-88bf-a2c42189e3d9` 的清单/文件校验前后一致。
PostgreSQL、API、reservation-worker 容器编号前后一致。

## 复核

```bash
docker compose --profile warehouse run --rm warehouse-report
docker compose --profile warehouse run --rm warehouse pipeline-status --run e57c518d-a981-4c2d-bf1b-cb95c0b5f6bd
docker compose --profile warehouse run --rm warehouse verify-aggregate --run 7c0290a5-7f09-4df6-b3b4-f1a3ff44d4e7
```

运行方法与实现说明见 [第五步](../warehouse/08_sales_publication.md)、[第六步](../warehouse/08_pipeline.md)。
