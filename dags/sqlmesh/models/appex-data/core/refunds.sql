MODEL (
  kind VIEW
);

-- adapty refunds
SELECT 
	app_id,
  app_name,
	event_date,
  -- install_date,
	COUNT (1) as refund_count,
	-SUM(proceeds_usd) as refund_amount
FROM `appex-data.base_revenue.adapty`
WHERE environment = 'Production'
AND event_type = 'subscription_refunded'
GROUP BY 1,2,3,4

UNION ALL

-- revenuecat refunds
SELECT 
	'' as app_id,
  '' as app_name,
	refund_date as event_date,
  -- install_date,
	COUNT (1) as cnt,
	SUM (refund_amount) as refund_amount
FROM `appex-data.base_revenue.revenuecat`
WHERE is_sandbox = false
AND refund_date IS NOT NULL
GROUP BY 1,2,3,4;