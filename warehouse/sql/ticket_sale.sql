-- 退款不删除原销售；金额取票级下单快照，不重复整单金额。
SELECT t.ticket_id, i.item_id, o.order_id, p.payment_id, i.session_seat_id,
       o.session_id, o.user_id, o.paid_at,
       to_date(from_utc_timestamp(o.paid_at, 'Asia/Shanghai')) AS paid_date,
       CAST(date_format(from_utc_timestamp(o.paid_at, 'Asia/Shanghai'), 'yyyyMMdd') AS INT) AS paid_date_key,
       CAST(1 AS INT) AS sale_quantity, CAST(i.unit_price AS DECIMAL(18,2)) AS sale_amount
FROM ods_ticket t
JOIN ods_booking_item i
  ON i.item_id=t.item_id AND i.order_id=t.order_id AND i.session_seat_id=t.session_seat_id
JOIN ods_booking_order o ON o.order_id=i.order_id AND o.session_id=i.session_id
JOIN ods_payment_attempt p ON p.order_id=o.order_id AND p.status='SUCCEEDED'
WHERE o.paid_at IS NOT NULL
