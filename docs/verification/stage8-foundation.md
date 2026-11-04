# 第八阶段前两步验证

日期：2026-11-04（Asia/Shanghai）。本地平台：macOS / Apple Silicon ARM64。
基线：main 的 085ad70cefbddb3705f83c6ae957c8c97b0adcd2；分支：codex/warehouse-stage8。

## 实现范围

- 固定 sales-v1 三个历史销售指标，明确票级粒度、paid_at 日期、退款后保留销售及 Decimal 金额。
- 增加独立 warehouse 命令、Spark 镜像和 warehouse_data 卷。
- 新增 006 空 schema 迁移，以及 reader / writer / reporter 账号配置。
- 真实数据抽取、加工、汇总发布尚未实现。

## 临时项目部署验收

执行 `python3 scripts/verify_deployment.py`。
临时项目：ticketing-verify-c87533bfc3c0442f；迁移版本数：6。

- 62 项 Python / PostgreSQL 集成测试全部通过，其中 5 项为数仓账号测试。
- 数仓管理员命令初始化和重复初始化通过。
- 未来表、序列和 ADS 视图的默认授权通过。
- 未授权的认证表读取、业务写入、建表和底层汇总访问被拒绝。
- 未标记角色冲突导致整个授权事务回滚，原密码仍能登录。
- 原有 SQL 约束、HTTP 注册登录、模拟支付、退款、重售、过期清理通过。
- 临时容器、数据卷和测试数据库清理通过。

完整测试输出见 [stage8-foundation-tests.txt](stage8-foundation-tests.txt)。

## 独立 Spark 环境

构建 `docker/warehouse.Dockerfile` 后，用无外部网络、主机名为 localhost 的临时容器执行 `check-env`。

- Java：openjdk version "17.0.21" 2026-10-20。
- PySpark：4.0.1；Py4J：0.10.9.9；PyArrow：21.0.0；Psycopg：3.2.13。
- Spark local[2]、Python worker 真正执行通过。
- Decimal 100.10 + 0.00 精确合计通过。
- 上海午夜日期边界通过。
- PyArrow → Spark → Parquet → PyArrow 往返通过。
- 非 root 用户写入并清理临时文件通过。
- 检查只使用固定样例，不访问业务数据。

初始化后又通过项目 Compose 和实际 warehouse_data 卷执行同样检查，结果 passed。
CI 已增加 Ubuntu AMD64 环境检查作业；本次记录仅证明本地 ARM64 运行结果。

## 开发环境初始化与回溯

在修改开发库之前生成并检查了自定义格式备份：
`/Users/Homework/ticketing-warehouse/backups/pre-warehouse-stage8-20261104-184140.dump`，大小 73267 字节。备份不进入 Git 或 Docker 构建上下文。

仅追加了三个独立数仓密码到已有 .env，不覆盖已有配置，不将密码输出到日志。
已在开发库应用 006 迁移并配置三类账号；reader、writer 使用自己的凭据连接检查通过。
PostgreSQL、API、reservation-worker 原容器 ID 前后完全一致，未重建常驻服务。

Git 切换分支只切换代码，不撤销数据库迁移或还原数据卷。原代码仍由 main 和上述基线提交保留。
