PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS merchant_health_features;

CREATE TABLE merchant_health_features AS
WITH
params AS (
  SELECT
    45 AS prediction_day,
    90 AS label_horizon_days,
    29.0 AS pro_price
),

frame AS (
  SELECT
    m.merchant_id,
    date(m.install_date) AS install_date,
    datetime(m.install_date, '+' || (SELECT prediction_day FROM params) || ' days') AS prediction_ts,
    datetime(m.install_date, '+' || (SELECT label_horizon_days FROM params) || ' days') AS label_ts,
    m.country,
    m.acquisition_channel,
    m.industry
  FROM dim_merchants m
),

pro_flag AS (
  SELECT
    f.merchant_id,
    CASE WHEN EXISTS (
      SELECT 1
      FROM v_sub_events_clean s
      WHERE s.merchant_id = f.merchant_id
        AND s.event_timestamp <= f.prediction_ts
        AND s.event_type IN ('upgrade_pro')
    ) THEN 1 ELSE 0 END AS is_pro_by_pred
  FROM frame f
),

label AS (
  SELECT
    f.merchant_id,
    CASE WHEN EXISTS (
      SELECT 1
      FROM v_sub_events_clean s
      WHERE s.merchant_id = f.merchant_id
        AND s.event_timestamp <= f.label_ts
        AND s.event_type = 'cancel'
    ) THEN 1 ELSE 0 END AS churn_by_90
  FROM frame f
),

app_30d AS (
  SELECT
    f.merchant_id,
    SUM(CASE WHEN e.event_type = 'dashboard_view'
              AND e.event_timestamp BETWEEN datetime(f.prediction_ts, '-30 days') AND f.prediction_ts
             THEN 1 ELSE 0 END) AS dashboard_views_30d,

    SUM(CASE WHEN e.event_type = 'sms_sent'
              AND e.event_timestamp BETWEEN datetime(f.prediction_ts, '-30 days') AND f.prediction_ts
             THEN 1 ELSE 0 END) AS sms_sent_30d,

    SUM(CASE WHEN e.event_type = 'sms_failed'
              AND e.event_timestamp BETWEEN datetime(f.prediction_ts, '-30 days') AND f.prediction_ts
             THEN 1 ELSE 0 END) AS sms_failed_30d,

    SUM(CASE WHEN e.event_type = 'integration_error'
              AND e.event_timestamp BETWEEN datetime(f.prediction_ts, '-30 days') AND f.prediction_ts
             THEN 1 ELSE 0 END) AS integration_errors_30d,

    MAX(CASE WHEN e.event_type = 'dashboard_view'
              AND e.event_timestamp <= f.prediction_ts
             THEN e.event_timestamp ELSE NULL END) AS last_dashboard_ts
  FROM frame f
  LEFT JOIN v_app_events_clean e
    ON e.merchant_id = f.merchant_id
  GROUP BY f.merchant_id
),

rev_30d AS (
  SELECT
    f.merchant_id,
    SUM(CASE WHEN date(r.event_date) BETWEEN date(f.prediction_ts, '-30 days') AND date(f.prediction_ts)
             THEN r.recovered_sales_nzd ELSE 0 END) AS recovered_sales_30d
  FROM frame f
  LEFT JOIN fact_recovered_sales_daily r
    ON r.merchant_id = f.merchant_id
  GROUP BY f.merchant_id
)

SELECT
  f.merchant_id,
  f.install_date,
  f.prediction_ts,
  f.label_ts,
  f.country,
  f.acquisition_channel,
  f.industry,

  p.is_pro_by_pred,

  a.dashboard_views_30d,
  a.sms_sent_30d,
  a.sms_failed_30d,
  a.integration_errors_30d,

  CASE
    WHEN a.sms_sent_30d > 0 THEN CAST(a.sms_failed_30d AS REAL) / CAST(a.sms_sent_30d AS REAL)
    ELSE 0.0
  END AS sms_fail_rate_30d,

  CASE
    WHEN a.last_dashboard_ts IS NULL THEN NULL
    ELSE CAST(julianday(f.prediction_ts) - julianday(a.last_dashboard_ts) AS INTEGER)
  END AS days_since_last_dashboard,

  r.recovered_sales_30d,

  CASE
    WHEN p.is_pro_by_pred = 1 THEN r.recovered_sales_30d / (SELECT pro_price FROM params)
    ELSE NULL
  END AS roi_ratio_30d,

  l.churn_by_90
FROM frame f
LEFT JOIN pro_flag p ON p.merchant_id = f.merchant_id
LEFT JOIN app_30d a ON a.merchant_id = f.merchant_id
LEFT JOIN rev_30d r ON r.merchant_id = f.merchant_id
LEFT JOIN label l ON l.merchant_id = f.merchant_id;
