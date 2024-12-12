MODEL (
  kind FULL,
);

WITH import_data AS (
  SELECT 
    CAST(Date AS DATE) AS install_date
    , App_Name AS app_name
    , App_Apple_Identifier AS app_id
    , Download_Type AS download_type
    , App_Version AS app_version
    , Device AS device
    , Platform_Version AS platform_version
    , Source_Type source_type
    , Page_Type AS page_type
    , Pre_Order AS pre_order
    , Territory AS country
    , Counts AS installs
    , _FILE_NAME AS filename
    , CASE 
        WHEN SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(5)] = 'ONE_TIME_SNAPSHOT'
        THEN DATE(1970,1,1)                       -- Report Date ONE_TIME_SNAPSHOT
        ELSE DATE(
          CAST(SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(5)] AS INTEGER),
          CAST(SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(6)] AS INTEGER),
          CAST(SPLIT(_FILE_NAME, '/')[SAFE_OFFSET(7)] AS INTEGER)
        )                                         -- Report Date Daily Reports
    END AS report_date
  FROM `appex-data-imports.appstore_connect_analytics_reports_bucket.installs`
  WHERE Download_Type IN ('First-time download', 'Redownload')
)
, daily_installs AS (
  SELECT
    i.app_id,
    i.install_date,
    i.country,
    i.app_version,
    i.report_date,
    SUM(i.installs) as installs
  FROM import_data i
  GROUP BY ALL
  QUALIFY report_date = MAX(report_date) OVER (PARTITION BY app_id, install_date)
)
, main_app_version AS (
  SELECT 
    app_id,
    install_date, 
    app_version as main_app_version,
    installs,
    ROW_NUMBER() OVER (PARTITION BY app_id, install_date ORDER BY installs DESC) as rank
  FROM daily_installs
)
, final AS (
  SELECT 
    i.app_id,
    i.install_date,
    i.country,
    p.main_app_version as app_version,
    sum(i.installs) as installs
  FROM daily_installs i
  LEFT JOIN main_app_version p
    USING(app_id, install_date)
  WHERE p.rank = 1
  GROUP BY ALL
)
SELECT * FROM final;