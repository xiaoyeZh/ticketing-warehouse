SELECT s.session_id, s.activity_id, a.title AS activity_title,
       s.venue_id, v.name AS venue_name, v.address AS venue_address, a.organizer_id,
       s.start_time AS start_at, s.end_time AS end_at,
       to_date(from_utc_timestamp(s.start_time, 'Asia/Shanghai')) AS start_date,
       CAST(date_format(from_utc_timestamp(s.start_time, 'Asia/Shanghai'), 'yyyyMMdd') AS INT) AS start_date_key,
       s.status AS session_status
FROM ods_activity_session s
JOIN ods_activity a ON a.activity_id=s.activity_id
JOIN ods_venue v ON v.venue_id=s.venue_id
