INSERT INTO
  kpi.user_identity (
    source,
    external_user_id,
    internal_user_id,
    first_seen_at,
    last_seen_at
  )
SELECT
  'booking',
  e.client_id::TEXT,
  MAX(kpi.safe_extract_user_id (p.params)),
  MIN(e.date_time),
  MAX(e.date_time)
FROM
  yandex_metrika_booking.events e
  JOIN yandex_metrika_booking.event_params p ON p.watch_id = e.watch_id
  AND p.date_time = e.date_time
WHERE
  e.date_time >= % (start_date) s::date
  AND e.date_time < (% (end_date) s::date + INTERVAL '1 day')
GROUP BY
  e.client_id::TEXT
ON CONFLICT (source, external_user_id) DO
UPDATE
SET
  internal_user_id = COALESCE(
    EXCLUDED.internal_user_id,
    kpi.user_identity.internal_user_id
  ),
  last_seen_at = GREATEST(
    kpi.user_identity.last_seen_at,
    EXCLUDED.last_seen_at
  ),
  updated_at = NOW();