INSERT INTO
  kpi.user_activity (
    activity_date,
    event_time,
    source,
    product,
    platform,
    event_name,
    internal_user_id,
    external_user_id,
    session_id,
    event_source_id,
    event_uuid
  )
SELECT
  e.date_time::date,
  e.date_time,
  'booking',
  'oz',
  'web',
  o.event_type,
  ui.internal_user_id,
  e.client_id::TEXT,
  e.visit_id,
  e.watch_id::TEXT,
  'booking:' || e.watch_id::TEXT
FROM
  yandex_metrika_booking.events e
  JOIN kpi.oz_events_web o ON REPLACE(e.url, 'goal://booking.avaclinic.ru/', '') = o.event_type
  LEFT JOIN kpi.user_identity ui ON ui.source = 'booking'
  AND ui.external_user_id::TEXT = e.client_id::TEXT
WHERE
  e.date_time >= '{start_date}'::date
  AND e.date_time < ('{end_date}'::date + INTERVAL '1 day')
  AND e.is_page_view = FALSE
ON CONFLICT DO NOTHING;