WITH sales AS (
    SELECT paid_date, session_id, COUNT(*) AS sold_ticket_count,
           COUNT(DISTINCT order_id) AS paid_order_count,
           CAST(SUM(sale_amount) AS DECIMAL(28,2)) AS paid_amount
    FROM dwd_ticket_sale
    GROUP BY paid_date, session_id
)
SELECT s.paid_date, s.session_id, d.activity_id, d.activity_title,
       d.venue_id, d.venue_name, d.start_at AS session_start_at,
       s.sold_ticket_count, s.paid_order_count, s.paid_amount
FROM sales s JOIN dim_session d ON s.session_id=d.session_id
