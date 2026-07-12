INSERT INTO
  kpi.user_identity (
    source,
    external_user_id,
    internal_user_id,
    first_seen_at,
    last_seen_at
  )
SELECT
  'appmetrica',
  appmetrica_device_id::TEXT,
  MAX(profile_id),
  MIN(event_datetime),
  MAX(event_datetime)
FROM
  appmetrica.events
WHERE
  event_datetime >= % (start_date) s::date
  AND event_datetime < (% (end_date) s::date + INTERVAL '1 day')
  AND appmetrica_device_id IS NOT NULL
GROUP BY
  appmetrica_device_id::TEXT
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