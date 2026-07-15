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
  'web_lk',
  'lk',
  'web',
  'PageView',
  ui.internal_user_id,
  e.client_id::TEXT,
  e.visit_id,
  e.watch_id::TEXT,
  'web_lk:' || e.watch_id::TEXT
FROM
  yandex_metrika_web_lk.events e
  LEFT JOIN kpi.user_identity ui ON ui.source = 'web_lk'
  AND ui.external_user_id::TEXT = e.client_id::TEXT
WHERE
  e.date_time >= '{start_date}'::date
  AND e.date_time < ('{end_date}'::date + INTERVAL '1 day')
  AND e.is_page_view = TRUE
ON CONFLICT DO NOTHING;