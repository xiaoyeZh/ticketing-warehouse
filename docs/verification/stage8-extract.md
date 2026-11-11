# 第八阶段第三步验证：一致性抽取

日期：2026-11-11（Asia/Shanghai）；平台：macOS / Apple Silicon ARM64。
分支：codex/warehouse-stage8；本步基线提交：b741d53。

## 独立临时环境验收

入口：`python3 scripts/verify_deployment.py --warehouse`。
临时项目：ticketing-verify-e940d6e4a5cc4f52；迁移数：7。

- 原有 62 项 Python / PostgreSQL 集成测试通过，见 [回归输出](stage8-extract-legacy-tests.txt)。
- 新增 18 项抽取集成测试通过，见 [抽取测试输出](stage8-extract-tests.txt)。
- Spark 无外部网络环境检查通过。
- 实际 CLI 抽取两个不同 UUID 的批次，并对两者完成七文件校验。
- 原有 SQL 约束、HTTP 支付、退款、重售和超时释放验证通过。
- 所有本次临时容器、数据卷和测试数据库清理通过。

并发测试在读取 booking_order 后，由另一连接完成并提交支付。
旧快照的订单仍为 PENDING、支付和票数量为 0；新批次看到 PAID 和两张票，证明跨表读取未混合两个提交状态。

故障验证还包括：空表、Decimal、UTC 边界、UUID/中文/空值、零金额、失败支付和退款保留、schema 漂移、重复批次不覆盖、导出中断、正式目录生成后失败、成功回执丢失、状态不明确、文件/清单损坏以及路径限制。
成功回执丢失时保持 SUCCEEDED；无法确认状态时保持不可消费，返回 BATCH_STATUS_UNCERTAIN。

## 开发库实际运行

先创建并通过 pg_restore --list 检查了备份：
`/Users/Homework/ticketing-warehouse/backups/pre-warehouse-extract-20261111-170053.dump`，大小 77236 字节。
备份位于 Git/Docker 构建均忽略的 backups 目录。

随后执行 warehouse-admin，应用 007 技术表迁移并配置权限。
用实际 reader/writer 凭据运行 extract，再运行 verify-batch。两个命令成功退出，七文件校验通过。

- source_batch_id：`cc90492f-55bc-48aa-88bf-a2c42189e3d9`。
- manifest_path：`ods/cc90492f-55bc-48aa-88bf-a2c42189e3d9/manifest.json`，相对于 warehouse_data 卷内 /warehouse-data。
- manifest_sha256：`c3ac0210d36fefbf95bc8479b3e55d1129d567d2d747fe4c0523b8f67e38ee4e`。
- 数据库状态：SUCCEEDED；files_verified：7。
- 总来源记录数：60。

| 来源表 | 原始行数 |
|---|---:|
| activity | 2 |
| activity_session | 4 |
| booking_item | 18 |
| booking_order | 10 |
| payment_attempt | 10 |
| ticket | 14 |
| venue | 2 |

同一来源快照的对照合计：成功支付订单 7 单，历史成功出票 14 张，模拟支付金额 1540.00。
这些是来源侧对照值，不是新建的票级事实或日报，也未扣除后续退款。

原有常驻服务容器 ID 在初始化与抽取前后保持一致。没有向 Git 加入 .env、数据库备份或 ODS 原始数据。
当前验证为本地 ARM64；Ubuntu AMD64 已配置相同 CI 入口，远端结果以分支 CI 为准。

## 复核命令

在仓库根目录执行，可检查本次批次：

```bash
docker compose --profile warehouse run --rm warehouse verify-batch --batch cc90492f-55bc-48aa-88bf-a2c42189e3d9
```

运行 extract 会创建新批次，不覆盖本批次。源数据后续变化不改变已保存文件，但手工改动文件会导致校验失败。
实现和失败恢复边界见 [七表一致性抽取讲解](../warehouse/08_ods_extract.md)。
