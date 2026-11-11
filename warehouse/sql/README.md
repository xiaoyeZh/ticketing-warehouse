# 加工 SQL 目录

第四步已实现 `dim_date.sql`、`dim_session.sql`、`ticket_sale.sql`，由 transform 命令读取已验证 ODS 并运行。
销售金额使用 booking_item.unit_price；支付日期统一使用 booking_order.paid_at 转上海日期；退款保留原销售。
加工之前和之后分别执行来源质量检查与来源合计对账，详见 warehouse/quality.py。
第五步的 session_sales_day.sql 按上海支付日期×场次汇总，经 aggregate 校验装载 DWS；publish 切换发布指针，ADS 只展示已发布结果。
