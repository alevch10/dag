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
  e.event_datetime::date,
  e.event_datetime,
  'appmetrica',
  'oz',
  'mobile',
  e.event_name,
  ui.internal_user_id,
  e.appmetrica_device_id::TEXT,
  e.session_id::NUMERIC,
  e.uuid::TEXT,
  'appmetrica:' || e.uuid::TEXT
FROM
  appmetrica.events e
  JOIN kpi.oz_events_mobile allowed ON allowed.event_type::TEXT = e.event_name::TEXT
  LEFT JOIN kpi.user_identity ui ON ui.source = 'appmetrica'
  AND ui.external_user_id::TEXT = e.appmetrica_device_id::TEXT
WHERE
  e.event_datetime >= % (start_date) s::date
  AND e.event_datetime < (% (end_date) s::date + INTERVAL '1 day')
ON CONFLICT DO NOTHING;