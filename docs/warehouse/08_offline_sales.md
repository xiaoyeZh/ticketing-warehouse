# 第八阶段：离线销售数仓

本章提供指标口径和运行说明。要了解命令如何调用代码、Spark 样例怎样执行，以及账号权限为何这样设计，请阅读 [第八阶段前两步：实现讲解](08_implementation_walkthrough.md)。

## 当前已实现的范围

目前已实现六步：指标口径、独立运行环境和账号、七表一致性抽取、Spark 维度与票级事实、每日场次汇总与报表发布，以及完整运行/重跑和端到端验收。

- 指标契约：`warehouse/contracts/sales_metrics.json`，版本 `sales-v1`。
- 命令：`metrics`、`check-env`、管理员专用的 `init-db`、`extract`、`verify-batch`、`transform`、`verify-transform`、`aggregate`、`verify-aggregate`、`publish`、`report`、`run`、`replay`、`pipeline-status`。
- 独立 Spark 镜像、数据卷和三类数据库账号。
- 006 迁移只创建 `wh_meta`、`dws`、`ads` 命名空间，不创建销售数据表，不修改业务记录。
- 007 迁移新增 `wh_meta.extract_batch` 技术表；抽取生成七张来源表的 Parquet、清单、参考合计和校验值。详见 [第三步实现讲解](08_ods_extract.md)。
- 008 迁移新增 `wh_meta.transform_run`；Spark 生成 dim_date、dim_session、dwd_ticket_sale 三表 Parquet，并记录血缘与质量合计。详见 [第四步实现讲解](08_sales_model.md)。

- 009 迁移新增版本化 DWS 日报、汇总/流水线记录、发布指针与事件、ADS 视图。详见 [第五步：日报汇总与发布](08_sales_publication.md) 和 [第六步：完整流水线与验收](08_pipeline.md)。

日常使用 `docker compose --profile warehouse run --rm warehouse run` 生成并发布当前快照，使用 `docker compose --profile warehouse run --rm warehouse-report` 查询报表。

## 第一步：sales-v1 指标字典

### 业务过程与粒度

统计本地模拟支付成功产生的历史销售。事实表一行代表一张成功售出的电子票，唯一业务键为 `ticket_id`。
同一座位退款后重新出售，会产生另一张票，应记录两次成功销售，不能按 `session_seat_id` 去重。

汇总粒度为 `paid_date × session_id`。现有一个订单只属于一个场次，每个订单最多一笔成功支付。

| 指标 | 英文字段 | 计算口径 |
|---|---|---|
| 成功售票数 | sold_ticket_count | 每个汇总组内的唯一销售事实行数 |
| 成功支付订单数 | paid_order_count | 每个汇总组内 `order_id` 的去重数 |
| 模拟支付金额 | paid_amount | 每个汇总组内 `booking_item.unit_price` 的总和 |

### 事件时间和金额

- 业务事件时间统一使用 `booking_order.paid_at`；尝试时间 `payment_attempt.created_at` 和出票时间 `ticket.issued_at` 另有含义，不用于这三个指标的日期归属。
- 原始时间保存 UTC，业务日期统一转换为 Asia/Shanghai。日期范围采用本地日期的左闭右开区间。
- 票级金额来自下单快照 `booking_item.unit_price`，不取现价，不把订单整单金额复制到每张票上。
- 金额使用 Decimal/NUMERIC；票级使用 DECIMAL(18,2)，汇总使用 DECIMAL(28,2)。禁止经过 float。
- 金额代表本项目模拟支付的原始数值；源系统尚未建模币种，不进行币种转换，也不将其标记为会计收入。

### 成功条件和特殊情况

- 销售必须关联同一订单的 `SUCCEEDED` 支付，且订单的 `paid_at` 非空；同时关联完整的电子票、订单明细和场次。
- 退款后保留原销售，不过滤 `ticket.status='VALID'`，不要求订单当前状态仍为 `PAID`。本阶段不提供退款额或净额指标。
- 失败支付不计销售；同一个支付请求重放不重复计算。
- 零金额支付成功仍计票数和订单数，金额贡献 0.00。
- 没有销售的日期/场次不造事实行；整体合计查询应返回票数 0、订单数 0、金额 0.00。
- 数据缺关联、重复票键、成功支付不唯一或金额不平时，后续加工应报质量错误，不静默去重或丢弃。
- 指标只反映源快照中可见的已提交记录，不宣称历史日终状态。同一个原始批次和代码版本应复现相同业务结果。
- 活动、场馆描述按采集快照解释；首版不恢复源系统未记录的历史名称。

### 数据来源

允许读取 `booking_order`、`booking_item`、`payment_attempt`、`ticket`、`activity_session`、`activity`、`venue` 七张表。
本阶段不读取用户资料、认证凭据、会话令牌、退款尝试或库存状态。

### 后续加工的固定验收样例

以下是加工与报表的固定验收期望；单独的 check-env 仍只验证环境，不生成业务报表。

| 样例 | 成功售票数 | 成功支付订单数 | 模拟支付金额 |
|---|---:|---:|---:|
| 一单购买 100、120 两张票 | 2 | 1 | 220.00 |
| 同一订单先失败，再成功 | 2 | 1 | 220.00 |
| 上述订单随后整单退款 | 2 | 1 | 220.00 |
| 一个 100 元座位售出、退款、再售出 | 2 | 2 | 200.00 |
| 一张零金额票成功支付 | 1 | 1 | 0.00 |

UTC `2026-11-03 15:59:00+00` 归属上海 11 月 3 日，`16:00:00+00` 归属上海 11 月 4 日。
完整对账在同一抽取快照内计算：成功支付金额合计 = 票级金额合计 = 汇总金额合计。

## 第二步：独立运行环境

### 组件和目录

| 内容 | 当前实现 |
|---|---|
| Python | 与现有应用相同的 3.12.16 基础镜像 |
| Java | 容器内 OpenJDK 17 JRE，不依赖宿主机 Java |
| Spark | PySpark 4.0.1，local[2]，关闭 Spark UI |
| 文件格式 | PyArrow 21.0.0，Parquet，显式 Decimal 与 UTC 时间 |
| 数据库驱动 | Psycopg 3.2.13 |
| 运行用户 | 容器 warehouse 用户，UID/GID 10001:10001，带独立主目录 |
| 数据目录 | `/warehouse-data`，由独立 `warehouse_data` 命名卷持久保存 |

依赖固定在 `requirements-warehouse.txt`。Java 的安全更新随 Debian 软件源安装，不声称镜像构建位级可复现。
业务镜像只复制轻量数仓命令和契约；Java、Spark、PyArrow 安装在独立数仓镜像中。

`warehouse/config.py`、`bootstrap.py` 管理连接和账号；`runtime.py` 提供离线环境检查；`warehouse/sql/` 保存维度、票级事实和每日场次汇总 SQL。

### 构建和离线检查

在仓库根目录执行：

```bash
docker compose --profile warehouse build warehouse
docker compose --profile warehouse run --rm --no-deps warehouse metrics
docker compose --profile warehouse run --rm --no-deps warehouse check-env
```

首次构建需要下载 Java 和 Spark，镜像大于购票应用镜像。`warehouse` 是按需 profile，不会默认常驻。
环境检查不连接数据库；它通过两条固定样例验证：

1. Java 与 Spark 启动，Python worker 真正执行。
2. 100.10 与 0.00 保持 Decimal，合计为 100.10。
3. 上海午夜边界的日期归属正确。
4. PyArrow 写入 → Spark 读取/写入 → PyArrow 读回，字段值保持一致。
5. 非 root 用户可写数据目录，临时检查文件结束后清理。

成功时标准输出返回 JSON `status=passed`；Spark 自身日志可能出现在标准错误。
CI 的独立 `warehouse-environment` 作业使用无外部网络容器执行相同检查，并将主机名设为 localhost 以支持离线解析。

### 数据库初始化与权限

已有 `.env` 保留原值，只追加 `.env.example` 中的三项数仓密码，并为每类账号设置独立密码。
这些变量没有值时，原购票服务仍可照常使用；数仓管理员命令会拒绝初始化。

```bash
docker compose up -d --wait postgres
docker compose --profile warehouse run --rm --build warehouse-admin
```

`warehouse-admin` 使用现有管理员镜像，执行未应用迁移并配置账号。旧迁移内容不修改。
普通 `warehouse` 服务只接收 reader、writer 密码，不接收管理员或 reporter 密码。

| 账号 | 允许 | 不授予 |
|---|---|---|
| ticketing_wh_reader | public schema 的七张来源表 SELECT | 写业务表、读认证表或用户资料、建表 |
| ticketing_wh_writer | wh_meta、dws 中表的 SELECT/INSERT/UPDATE/DELETE，以及所需序列使用 | 读写业务表、建表、管理角色、修改 ADS 视图 |
| ticketing_wh_reporter | ads 中视图/表 SELECT | 访问底层汇总、来源表或写数据 |

第三至六步已新增抽取/加工/汇总/流水线技术表、DWS 日报和 ADS 视图；writer 负责装载与发布，reporter 通过独立 warehouse-report 服务查询。默认权限也覆盖后续对象。
后续迁移须继续由同一个初始化管理员创建对象；更换对象创建者时，需要重新配置该创建者的默认权限。
重复初始化复用本项目标记的账号；遇到未标记账号、特权账号或额外角色成员关系时停止，整个授权事务回滚。

### 验证

```bash
docker compose run --rm --build tests
python3 scripts/verify_deployment.py
```

新增权限集成测试使用随机账号与独立临时数据库，验证未来新对象的授权、越权被拒绝、初始化幂等和冲突回滚。
测试不改动开发库数仓账号的密码。部署验收使用临时 Compose 项目并清理自己的资源。

抽取测试使用独立数仓测试镜像：`docker compose --profile warehouse run --rm --build warehouse-tests`。
完整数仓与部署验收使用 `python3 scripts/verify_deployment.py --warehouse`，包含日报、原子发布、ODS 重跑和报表查询。

本地完整验证和初始化记录见 [第八阶段前两步验证](../verification/stage8-foundation.md)。

## 分支和版本回溯

本阶段从 `main` 的 `085ad70cefbddb3705f83c6ae957c8c97b0adcd2` 开始，当前工作分支为 `codex/offline-warehouse`。
该分支原名为 `codex/warehouse-stage8`，重命名保留了已有的提交历史；历史验证记录中的分支名对应验证发生时的名称。
完成并提交本分支改动后，可以切换查看：

```bash
git switch main
git switch codex/offline-warehouse
```

未提交的改动不属于某个分支的已保存历史，切换前应提交或妥善保留。不要用强制重置来切换版本。
Git 保存代码，不保存 PostgreSQL 数据、Docker 数据卷、个人 `.env` 或 ODS 文件。
切换旧代码不会自动撤销已执行的 006～009 数仓迁移；这些迁移新增分析对象，旧业务代码不使用它们。
需要恢复数据库历史状态时，另行使用对应备份，不能把切换分支当作数据库还原。

## 参考

- [PySpark 4.0.1 安装与支持版本](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html)
- [Spark 4.0.1 本地运行与 Java 支持](https://spark.apache.org/docs/4.0.1/)
- [PostgreSQL 17 默认权限](https://www.postgresql.org/docs/17/sql-alterdefaultprivileges.html)
