# 第八阶段第四步验证：Spark 销售建模

日期：2026-11-11（Asia/Shanghai）；本地平台：macOS / Apple Silicon ARM64。
分支：codex/warehouse-stage8；本步起点：60e307e。
第三步已推送并通过 [Linux CI](https://github.com/xiaoyeZh/ticketing-warehouse/actions/runs/42992639546)，三项作业均为 success。

## 独立环境验收

执行 `python3 scripts/verify_deployment.py --warehouse`。
临时项目：ticketing-verify-db8401c605014589；数据库迁移数：8。

- 原有交易与权限 62 项测试通过。
- ODS 抽取 18 项测试通过，新增真实 Spark 加工 15 项测试通过。
- 合计 95 项；[测试名与结果摘要](stage8-model-tests.txt)。
- 离线 Spark 环境、两次 ODS 抽取、同一输入的两次 CLI transform 和三表文件校验通过。
- 每次加工生成不同 run_id，质量合计与来源参考合计一致。
- HTTP 支付、退款、重售和后台过期释放回归通过。
- 本次临时容器、数据卷和测试数据库清理通过。

新增用例验证正常金额、退款重售与价格快照、零金额、上海午夜、空数据、相同 ODS 重跑、损坏来源、重复键、孤儿明细、重复成功支付、订单金额不平、参考合计不平、提交失败、成功回执丢失、ODS 中途变化和输出损坏。
重跑测试修改实时活动名后，结果仍使用原 ODS 名称。全部断言使用真实 Spark 与 PostgreSQL。
原始 unittest 输出包含 ResourceWarning 诊断；测试进程退出码为 0，全部断言通过，完整原始日志保存在本地 `artifacts/warehouse-model/` 供复核，摘要明确省略了重复诊断。
本记录不以本地结果替代远端平台验证，Linux 以本分支相应提交的 Actions 为准。

## 开发环境实际结果

应用 008 迁移前创建并检查备份：
`/Users/Homework/ticketing-warehouse/backups/pre-sales-model-20261111-174141.dump`，大小 81372 字节。
备份不进入 Git 或 Docker 构建上下文。

- 原 ODS source_batch_id：`cc90492f-55bc-48aa-88bf-a2c42189e3d9`。
- 加工 run_id：`71d9d7e7-f365-4413-919b-99bb113caaca`。
- 清单：`curated/cc90492f-55bc-48aa-88bf-a2c42189e3d9/71d9d7e7-f365-4413-919b-99bb113caaca/manifest.json`，相对于数据卷内 /warehouse-data。
- 代码 SHA-256：`3cda2ab3336a7f19cdcf689b7d3ef30d5929dbe921a7353fc11316e63495ebcf`。
- transform 和 verify-transform 成功，三个数据集校验通过。

| 数据集 | 行数 |
|---|---:|
| dim_date | 12 |
| dim_session | 4 |
| dwd_ticket_sale | 14 |

来源和加工一致：历史成功售票 14 张、成功支付订单 7 单、模拟支付金额 1540.00。
这代表历史成功销售，仍保留已退款的原销售，不是退款净额或日报。

加工前后 verify-batch 返回完全一致，原 ODS 文件与清单未变。
既有 PostgreSQL、API、reservation-worker 容器 ID 未变；业务记录没有被加工任务改写。
新增 PostgreSQL 对象仅为 wh_meta.transform_run 管理表，三张维度/事实结果为 Parquet 文件。

## 复核命令

```bash
docker compose --profile warehouse run --rm warehouse verify-transform --run 71d9d7e7-f365-4413-919b-99bb113caaca
```

同一 source_batch_id 再次 transform 会生成新 run，不覆盖当前结果。
详见 [第四步实现与使用](../warehouse/08_sales_model.md)。
