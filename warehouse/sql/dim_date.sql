-- 覆盖本批次支付日与场次开场日之间的连续日期；完全空输入得到有类型的空表。
WITH days AS (
    SELECT to_date(from_utc_timestamp(paid_at, 'Asia/Shanghai')) AS day
    FROM ods_booking_order WHERE paid_at IS NOT NULL
    UNION ALL
    SELECT to_date(from_utc_timestamp(start_time, 'Asia/Shanghai')) AS day FROM ods_activity_session
), bounds AS (
    SELECT min(day) AS first_day, max(day) AS last_day FROM days
), calendar AS (
    SELECT explode(sequence(first_day, last_day, INTERVAL 1 DAY)) AS calendar_date FROM bounds
)
SELECT CAST(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
       calendar_date, year(calendar_date) AS calendar_year, quarter(calendar_date) AS quarter,
       month(calendar_date) AS month, dayofmonth(calendar_date) AS day_of_month,
       weekday(calendar_date)+1 AS iso_weekday, weekofyear(calendar_date) AS iso_week_number,
       weekday(calendar_date)>=5 AS is_weekend
FROM calendar
