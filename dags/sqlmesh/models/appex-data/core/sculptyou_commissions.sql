MODEL (
  kind VIEW
);

WITH refunds AS (
  SELECT
    order_line_id,
    sum(quantity) AS refunded_quantity,
    sum(coalesce(subtotal, 0)) AS refunded_subtotal
  FROM `appex-data-imports.shopify_sculptyou_fivetran.order_line_refund`
  GROUP BY
    1
), order_items AS (
  SELECT
    orders.id AS order_id,
    orders.created_at AS order_date,
    orders.total_discounts,
    orders.customer_id,
    CASE
      WHEN MIN(orders.created_at) OVER (PARTITION BY orders.customer_id) = orders.created_at
      	THEN 'New sales'
	    ELSE 'Renewals'
	END AS sales_type,
    order_items.id AS order_line_id,
    order_items.name AS product_name,
    order_items.price,
    order_items.quantity
  FROM `appex-data-imports.shopify_sculptyou_fivetran.order` AS orders
  INNER JOIN `appex-data-imports.shopify_sculptyou_fivetran.order_line` AS order_items
    ON orders.id = order_items.order_id
  WHERE
    order_items.name IN ('Daily Sculpt 3-Month Membership!', 'Daily Sculpt Membership!', 'Daily Sculpt YEARLY Membership!', 'YEARLY Membership UPGRADE! (FOR CURRENT MONTHLY SUBSCRIBERS)')
)
SELECT
  order_items.order_date,
  order_items.product_name,
  order_items.sales_type,
  CASE
    WHEN order_items.order_date < '2024-11-01'
    THEN 0.15
    WHEN order_items.order_date >= '2024-11-01'
    THEN 0.20
  END AS commision_percent,
  sum(order_items.quantity) - ifnull(sum(refunds.refunded_quantity), 0) AS net_quantity,
  round(sum(order_items.price * order_items.quantity), 2) AS gross_sales,
  -ifnull(sum(refunds.refunded_quantity), 0) AS refunded_quantity,
  -round(ifnull(sum(refunds.refunded_subtotal), 0), 2) AS refunds,
  -round(sum(order_items.total_discounts), 2) AS discounts,
  round(
    sum(order_items.price * order_items.quantity) - ifnull(sum(refunds.refunded_subtotal), 0) - sum(order_items.total_discounts),
    2
  ) AS net_sales
FROM order_items
LEFT JOIN refunds
  USING (order_line_id)
GROUP BY
  1,
  2,
  3,
  4