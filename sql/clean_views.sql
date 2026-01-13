PRAGMA foreign_keys = ON;

DROP VIEW IF EXISTS v_sub_events_clean;
CREATE VIEW v_sub_events_clean AS
SELECT
  event_id,
  merchant_id,
  datetime(event_timestamp) AS event_timestamp,
  lower(event_type) AS event_type_raw,
  CASE
    WHEN lower(event_type) LIKE '%cancel%' THEN 'cancel'
    WHEN lower(event_type) LIKE '%trial%' THEN 'trial_start'
    WHEN lower(event_type) LIKE '%upgrade%' OR lower(event_type) LIKE '%subscription_start%' THEN 'upgrade_pro'
    ELSE lower(event_type)
  END AS event_type,
  monthly_price,
  plan_code_raw,
  CASE
    WHEN lower(coalesce(plan_code_raw, '')) LIKE '%pro%' THEN 'pro'
    WHEN lower(coalesce(plan_code_raw, '')) LIKE '%free%' THEN 'free'
    ELSE NULL
  END AS plan_tier
FROM fact_subscription_events;

DROP VIEW IF EXISTS v_app_events_clean;
CREATE VIEW v_app_events_clean AS
WITH normalized AS (
  SELECT
    event_id,
    merchant_id,
    datetime(event_timestamp) AS event_timestamp,
    lower(event_type) AS event_type_raw,
    CASE
      WHEN lower(event_type) LIKE '%dashboard%' THEN 'dashboard_view'
      WHEN lower(event_type) LIKE '%sms_failed%' OR lower(event_type) LIKE '%smsfail%' THEN 'sms_failed'
      WHEN lower(event_type) LIKE '%sms_sent%' OR lower(event_type) LIKE '%smssent%' THEN 'sms_sent'
      WHEN lower(event_type) LIKE '%integration%' THEN 'integration_error'
      ELSE lower(event_type)
    END AS event_type,
    metadata
  FROM fact_app_events
),
deduped AS (
  SELECT *
  FROM (
    SELECT
      merchant_id,
      event_timestamp,
      event_type,
      metadata,
      ROW_NUMBER() OVER (
        PARTITION BY merchant_id, event_timestamp, event_type
        ORDER BY event_id
      ) AS rn
    FROM normalized
  )
  WHERE rn = 1
)
SELECT
  merchant_id,
  event_timestamp,
  event_type,
  metadata
FROM deduped;
