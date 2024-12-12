MODEL (
  kind FULL,
  audits (
    not_null(columns := (transaction_id, original_transaction_id, event_date, report_date))
  ),
  partitioned_by event_date,
  -- physical_properties (
  --   require_partition_filter = true
  -- )
);

SELECT 
  -- events.*
  profile_id
  , event_type
  , event_datetime as event_ts
  , event_datetime::DATE as event_date
  , transaction_id
  , original_transaction_id
  , subscription_expires_at
  , environment
  , revenue_usd
  , proceeds_usd
  , net_revenue_usd
  , tax_amount_usd
  , revenue_local
  , cancellation_reason
  , proceeds_local
  , net_revenue_local
  , tax_amount_local
  , customer_user_id
  , store
  , product_id
  , developer_id
  , ab_test_name
  , ab_test_revision
  , paywall_name
  , paywall_revision
  , profile_country
  , install_date::DATE
  , idfv
  , idfa
  , advertising_id
  , ip_address
  , android_app_set_id
  , android_id
  , device
  , currency
  , store_country
  , attribution_source
  , attribution_network_user_id
  , attribution_status
  , attribution_channel
  , attribution_campaign
  , attribution_ad_group
  , attribution_ad_set
  , attribution_creative
  , CAST(
        MIN(CASE WHEN events.transaction_id = events.original_transaction_id THEN events.event_datetime END) 
          OVER (PARTITION BY events.original_transaction_id)
      AS DATE) 
    AS original_transaction_date
    , CAST(
        MIN(CASE WHEN events.transaction_id = events.original_transaction_id THEN events.event_datetime END) 
          OVER (PARTITION BY events.original_transaction_id)
      AS TIMESTAMP) 
    AS original_transaction_ts
  , _FILE_NAME AS filename
  , SPLIT(SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(3)], '/')[SAFE_OFFSET(0)] AS gcs_folder
  , SPLIT(SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(4)], '_')[SAFE_OFFSET(0)] AS adapty_id
  , PARSE_DATE('%Y-%m-%d', SPLIT(SPLIT(SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(4)], '_')[SAFE_OFFSET(1)], '.')[SAFE_OFFSET(0)]) AS report_date
  , map.app_id
  , map.app_name
 FROM `appex-data-imports.adapty_bucket.events` events
 LEFT JOIN `appex-data.base.app_ids` map on map.adapty_id = SPLIT(SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(4)], '_')[SAFE_OFFSET(0)]
 WHERE events.environment = 'Production';