MODEL (
  kind FULL,
  grain (transaction_id, report_date),
  audits (
    not_null(columns := (transaction_id, original_transaction_id, event_date, report_date))
  ),
  partitioned_by event_date  
);

WITH import_data AS (
  SELECT
    *,
    _FILE_NAME AS filename,
    PARSE_DATE('%Y-%m-%d', SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(4)]) AS report_date,
    SPLIT(SPLIT(SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(5)], '_')[SAFE_OFFSET(1)], '.')[SAFE_OFFSET(0)] AS report_timestamp
  FROM `appex-data-imports.revenuecat_bucket.transactions`
), dedupe_events AS (
  SELECT
    store_transaction_id AS transaction_id,
    rc_last_seen_app_user_id_alias AS user_id,
    original_store_transaction_id AS original_transaction_id,
    MIN(
      CASE WHEN store_transaction_id = original_store_transaction_id THEN start_time END
    ) OVER (PARTITION BY original_store_transaction_id)::DATE AS original_transaction_date,
    MIN(
      CASE WHEN store_transaction_id = original_store_transaction_id THEN start_time END
    ) OVER (PARTITION BY original_store_transaction_id)::TIMESTAMP AS original_transaction_ts,
    first_seen_time::DATE AS install_date,
    first_seen_time::TIMESTAMP AS install_ts,
    start_time::DATE AS event_date,
    start_time::TIMESTAMP AS event_ts,
    MAX(unsubscribe_detected_at) OVER (PARTITION BY store_transaction_id)::TIMESTAMP AS unsubscribe_detected_at,
    MAX(billing_issues_detected_at) OVER (PARTITION BY store_transaction_id)::TIMESTAMP AS billing_issues_detected_at,

    MAX(refunded_at) OVER (PARTITION BY store_transaction_id)::DATE AS refund_date,
    MAX(refunded_at) OVER (PARTITION BY store_transaction_id)::TIMESTAMP AS refund_ts,
    CASE WHEN MAX(refunded_at) OVER (PARTITION BY store_transaction_id) IS NOT NULL THEN purchase_price_in_usd - price_in_usd ELSE 0 END AS refund_amount,
    
    DATE_DIFF(start_time, first_seen_time, DAY) AS install_age,
    store,
    country,
    platform,
    product_identifier,
    product_duration,
    renewal_number,
    price_in_usd,
    report_date,
    is_trial_conversion,
    is_trial_period,
    is_sandbox,
    ROW_NUMBER() OVER (PARTITION BY store_transaction_id ORDER BY report_date DESC) AS rn
  FROM import_data
  WHERE
    TRUE AND is_sandbox = FALSE
)
SELECT
  *
  EXCEPT (rn),
  MIN(CASE WHEN price_in_usd > 0 THEN renewal_number END) OVER (PARTITION BY original_transaction_id) = renewal_number AS is_first_usd_renewal
FROM dedupe_events
WHERE
  rn = 1