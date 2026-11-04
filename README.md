# Ticketing Warehouse

活动票务交易与运营离线数仓项目。以数据库课程项目为起点，逐步扩展为数据开发与数据仓库作品。

## 当前进度

已完成 Docker + PostgreSQL、十四张业务与认证表、下单锁座、模拟支付、出票、退款、FastAPI 接口和注册登录。已提供蓝色主题的轻量购票页面，CLI、HTTP 和网页均可演示交易流程。
提供迁移登记、可重复样例、受限应用数据库账号，以及真实 PostgreSQL 双连接并发验证。
离线数仓已完成第八阶段前两步：销售指标口径、独立 Spark 运行环境和受限账号初始化；真实数据抽取、加工和报表尚未实现。
第一版不包含检票与核销；支付和退款为本地同步模拟，不发生真实资金交易。

## 本地快速部署

需要 Git、已运行的 Docker Desktop（或 Docker Engine + Compose v2）以及首次下载镜像的网络。
无需在本机安装 Python、PostgreSQL 或 psql。

```bash
git clone https://github.com/xiaoyeZh/ticketing-warehouse.git
cd ticketing-warehouse
cp .env.example .env
docker compose up -d --wait --wait-timeout 120 postgres
docker compose run --rm --build db-admin
docker compose run --rm db-admin demo
docker compose --profile workers up -d --wait --wait-timeout 120 api reservation-worker
```

私有仓库克隆需要 GitHub 身份验证。`main` 包含当前已验证的 API、轻量前端与简 / 繁 / EN 语言切换功能。

启动后打开 http://127.0.0.1:8000/，通过页面注册登录、选场次和座位、下单、模拟支付、查看电子票及退款。
网页操作见 [前端使用说明](docs/web/06_web.md)。原有接口文档位于 http://127.0.0.1:8000/docs。

已有 `.env` 时保留原文件，不再次复制覆盖。新增应用密码配置见 `.env.example`；本次本地开发已自动补齐。
`.env` 存放个人配置，不提交 Git、不进入 Docker 构建上下文。
示例密码供本地体验，正式共享实例应配置独立密码。

启动后数据库默认监听本机 `127.0.0.1:15432`，端口冲突时修改 `.env` 中的 POSTGRES_PORT。
数据库名默认 `ticketing`；本机数据库客户端使用初始化管理员和 `.env` 中的密码。
容器间使用 `postgres:5432`，数据库时区为 UTC，catalog 按 Asia/Shanghai 展示场次开始时间。

## 服务分工

| Compose 服务 | 作用 | 运行方式 |
|---|---|---|
| `postgres` | PostgreSQL 17.11 | 默认常驻，命名卷持久化 |
| `api` | 购票网页、FastAPI 接口、注册登录和 Swagger 文档 | 本机 127.0.0.1:8000 |
| `db-admin` | 迁移与演示数据初始化 | 按需运行，使用初始化管理员 |
| `app` | 下单、模拟支付、退款、查询和清理 CLI | 按需运行，使用 ticketing_app |
| `reservation-worker` | 每五秒处理过期订单 | workers profile，可常驻 |
| `tests` | 独立临时数据库中的集成测试 | 按需运行 |
| `warehouse` | 独立 Spark 环境检查与指标契约 | warehouse profile，按需运行 |
| `warehouse-admin` | 数仓命名空间和三类受限账号初始化 | warehouse profile，管理员按需运行 |

Python 镜像固定为 3.12.16-slim-bookworm；Psycopg 3.2.13。
应用数据库账号没有建表、删除交易记录或修改用户角色权限。HTTP 订单接口从登录令牌识别用户，不能指定他人的用户编号。CLI 保留为本地教学工具。

## 轻量购票页面

首页提供可售活动、场次搜索和真实座位选择；“我的订单”提供倒计时、取消、模拟支付、电子票和整单退款。
页面采用蓝色主题，暂不设置品牌名称或 logo，支持桌面和手机访问。
右上角支持简 / 繁 / EN 切换，弹窗也支持；浏览器会记住语言偏好。活动名称和用户填写内容保留原文。
不需要前端构建、Node 运行环境或额外容器，也不依赖在线字体和图标脚本。
登录凭据仅保留在当前页面内存，刷新后需重新登录；主动退出会撤销会话。

已有 API 环境升级前端，只需：

```bash
docker compose up -d --build --wait --wait-timeout 120 api
```

没有新增迁移，原有数据与接口保留。详细操作见 [前端说明](docs/web/06_web.md)。

## HTTP 接口与登录

- `/activities`、`/sessions` 和 `/sessions/{session_id}/seats` 可公开查询可售活动与库存。
- `/auth/register` 注册普通用户，`/auth/login` 获取一小时有效的 Bearer 令牌，`/auth/me` 查询当前用户，`/auth/logout` 撤销当前令牌。
- `/orders` 创建或列出自己的订单；`/orders/{order_id}` 查询详情。
- `/orders/{order_id}/cancel`、`/payments`、`/refunds` 分别取消、模拟支付和整单退款。
- 订单操作要求登录，他人的订单返回 404；JSON 不接受额外 user_id 或 role。
- HTTP 金额使用字符串，例如 `"220.00"`；下单、支付和退款要求 UUID 请求编号，重试复用原值。

无需本机 Python 的完整 HTTP 演示：

```bash
docker compose run --rm --entrypoint python app scripts/api_demo.py --base-url http://api:8000
```

脚本随机注册两个演示用户，运行支付、退款和重售，结束后退款释放座位并退出会话，不输出密码或令牌。
已有 CLI 用户没有自动分配密码，注册不能覆盖这些账号；API 演示使用新账号。
API 仅绑定本机，修改 `.env` 的 API_PORT 可调整宿主机端口。接口、错误码及开发说明见 [API 文档](docs/api/05_fastapi.md)。

已有第四阶段环境升级，保留 `.env` 和数据卷：

```bash
docker compose stop reservation-worker
docker compose run --rm --build db-admin
docker compose --profile workers up -d --wait --wait-timeout 120 api reservation-worker
```

## 下单与取消

先运行 catalog 查看实际编号。下面中文占位符需要替换为输出中的数字：

```bash
docker compose run --rm app reserve --user 用户编号 --session 场次编号 --seats 库存编号A 库存编号B
docker compose run --rm app order --user 用户编号 --order 订单编号
docker compose run --rm app cancel --user 用户编号 --order 订单编号
```

座位参数使用 session_seat_id，而非物理 seat_id。
默认占用十分钟；只有已发布、开售、尚未开始且场馆和座位启用的场次才能下单。
同一订单的座位必须全部可售，否则完整回滚。
相同用户重试同一操作使用原 `--request-id` UUID；更换场次或座位集合需要新请求编号。

快速过期演示可在 reserve 后加 `--hold-seconds 2`，超过截止时间后：

```bash
docker compose run --rm app expire
```

启用或停止自动清理：

```bash
docker compose --profile workers up -d --build reservation-worker
docker compose logs --tail=20 reservation-worker
docker compose stop reservation-worker
```

## 模拟支付与退款

先下单，再用订单输出的总额支付。下列中文占位符要替换为实际值；支付和退款分别使用独立的 UUID，重试时复用原值。

```bash
docker compose run --rm app pay --user 用户编号 --order 订单编号 --amount 订单总额 --request-id 支付UUID
docker compose run --rm app order --user 用户编号 --order 订单编号
docker compose run --rm app refund --user 用户编号 --order 订单编号 --reason "行程改变" --request-id 退款UUID
```

支付成功后每个订单明细生成一张 VALID 电子票，库存进入 SOLD。
开场前支持整单全额退款，成功后票变为 REFUNDED，库存恢复 AVAILABLE，可重新购买。
支付和退款命令加 `--outcome FAILED` 可模拟失败；失败尝试保留，原订单与库存状态保持，成功重试使用新的 UUID。
`order` 输出支付尝试、票、退款尝试和状态历史；模拟失败需要查看业务状态，CLI 退出码 0 表示操作已记录。
详见 [支付退款学习与部署说明](docs/database/04_payment_refund.md)。

已有第三阶段环境升级：

```bash
docker compose stop reservation-worker
docker compose run --rm --build db-admin
docker compose --profile workers up -d --build reservation-worker
```

保留现有 `.env` 和命名卷；迁移会扩展表和权限，不清空数据。

## 迁移和样例

`db-admin` 按顺序执行 migrations，并在 schema_migration 保存校验值。
重复运行跳过已执行版本；不能通过修改旧迁移来更新已有库，应新增迁移。
早期已经手工执行 001/002 的本地库会先核对已知结构，再登记旧版本并依次应用后续迁移；不重建旧表。

| 迁移 | 内容 |
|---|---|
| 001 | 用户、场馆、物理座位、活动、场次 |
| 002 | 场次库存与独立定价 |
| 003 | 订单、明细、状态历史、库存锁座关联 |
| 004 | 模拟支付、电子票、退款与已售库存归属 |
| 005 | Argon2id 登录凭据和可过期、可撤销的会话 |
| 006 | 数仓 wh_meta、dws、ads 空命名空间 |

`db-admin demo` 保留历史场次，创建或复用未来演示场次和两个普通用户。
重复运行不重置已有价格或占用状态；没有未来演示场次时创建新的未来场次。
`seeds/001_ticketing_demo.sql` 是早期固定日期样例，保留用于学习，当前下单演示使用动态未来样例。

## 测试

```bash
docker compose run --rm --build tests
```

测试自动创建独立的 ticketing_test_* 数据库，使用真实数据库连接验证事务、权限、并发和幂等；结束后只删除该测试库。
并发证据包含数据库后端编号、Lock 等待和阻塞者编号。
验证日志和本地演示结果保存在 [docs/verification](docs/verification/)。

前端开发检查（需要 Node.js 22+，运行网页不需要）：

```bash
node --check ticketing/web/app.js
node --test tests/js/web.test.mjs tests/js/i18n.test.mjs
```

每次推送或提交 PR，GitHub Actions 自动运行前端检查与 Docker / PostgreSQL 部署验收。
在本机用全新配置和数据卷重现部署验收（另需 Python 3.10+，仅使用标准库）：

```bash
python3 scripts/verify_deployment.py
```

验收使用独立项目、随机端口与临时数据卷，结束后清理本次资源，报告写入 `artifacts/deployment/`。
执行范围、日志和 CI 使用方法见 [自动化测试与部署验收](docs/development/07_ci_deployment.md)。

早期 SQL 约束脚本也可通过管理员 psql 在开发环境执行，样例在末尾回滚：

```bash
docker compose exec -T postgres psql -X -U ticketing_admin -d ticketing -v ON_ERROR_STOP=1 < tests/sql/001_core_tables.sql
docker compose exec -T postgres psql -X -U ticketing_admin -d ticketing -v ON_ERROR_STOP=1 < tests/sql/002_session_seat.sql
```

上述输入重定向适用于 macOS/Linux。Windows 可用 docker compose cp 将 SQL 复制到容器，再用 psql -f 执行。
如自定义了库名或管理员名，替换 psql 命令中的参数。

## 数据保存与常用操作

```bash
docker compose ps
docker compose logs --tail=50 postgres
docker compose exec postgres psql -U ticketing_admin -d ticketing
```

psql 中用 `\dt` 列表，`\d booking_order` 查看结构，`\q` 退出。
现在有十四张业务与认证表及 schema_migration 技术表。
数据库文件保存在 ticketing-warehouse_pg_data 命名卷中；重建容器保留数据，备份仍需单独进行。
日常停止使用 docker compose stop；docker compose down 保留命名卷，down -v 则会删除数据卷。
POSTGRES_* 初始化账号和库名只在首次创建数据卷时生效，之后修改 .env 不会自动修改已有管理员密码。

## 学习文档

- [五张基础表](docs/database/01_core_tables.md)
- [场次库存与组合外键](docs/database/02_session_seat.md)
- [订单事务、双连接并发与部署说明](docs/database/03_booking_transactions.md)
- [模拟支付、电子票、整单退款与升级演示](docs/database/04_payment_refund.md)
- [FastAPI 接口、登录、权限与 HTTP 演示](docs/api/05_fastapi.md)
- [轻量前端、网页购票与响应式布局](docs/web/06_web.md)
- [GitHub Actions、全新部署验收与开发顺序](docs/development/07_ci_deployment.md)
- [第八阶段：销售指标口径、独立 Spark 环境与数仓账号](docs/warehouse/08_offline_sales.md)
- [第八阶段前两步实现讲解：命令、Spark 样例、迁移与账号权限](docs/warehouse/08_implementation_walkthrough.md)

票务功能已在 macOS / Apple Silicon ARM64 和 GitHub Actions Ubuntu 24.04 / AMD64 验证。
新增 Spark 环境已在本地 ARM64 验证，并已配置 Ubuntu AMD64 CI 检查；远端结果以本分支 CI 为准。
第八阶段前两步的测试与初始化记录见 [验证文档](docs/verification/stage8-foundation.md)。其他宿主系统尚未进行运行验证。
首次拉取官方镜像遇到连接中断时，可检查 Docker Desktop 网络或代理后重试构建。

## Anaconda 本地开发

项目提供 [environment.yml](environment.yml)，创建独立的 ticketing-warehouse 环境，使用 Python 3.12.16 和固定项目依赖。
本机环境已创建；激活使用 `conda activate ticketing-warehouse`。
其他电脑在仓库根目录执行 `conda env create --file environment.yml`。
VS Code 解释器选择与本机数据库连接参数见 [Conda 开发说明](docs/development/conda.md)。
