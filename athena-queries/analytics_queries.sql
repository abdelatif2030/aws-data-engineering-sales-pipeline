-- =============================================================================
-- AWS Glue Sales ETL Project - Athena Analytics Queries
-- Database: sales_db | Table: sales (Parquet, partitioned year/month)
-- Run these in Amazon Athena Query Editor or via CLI
-- =============================================================================

-- Q1: Overall pipeline health check
SELECT
    COUNT(*)                          AS total_orders,
    COUNT(DISTINCT customer_id)       AS unique_customers,
    COUNT(DISTINCT product_id)        AS unique_products,
    ROUND(SUM(gross_revenue), 2)      AS total_gross_revenue,
    ROUND(SUM(discount_amount), 2)    AS total_discounts,
    ROUND(SUM(net_revenue), 2)        AS total_net_revenue,
    ROUND(SUM(gross_profit), 2)       AS total_profit,
    ROUND(AVG(gross_margin_pct)*100,1)AS avg_margin_pct
FROM sales;

-- Q2: Revenue by product category with discount analysis
SELECT
    category,
    COUNT(*)                          AS orders,
    SUM(quantity)                     AS units_sold,
    ROUND(SUM(net_revenue), 2)        AS revenue,
    ROUND(SUM(gross_profit), 2)       AS profit,
    ROUND(AVG(discount_pct), 1)       AS avg_discount_pct,
    ROUND(SUM(discount_amount), 2)    AS discount_given
FROM sales
GROUP BY category
ORDER BY revenue DESC;

-- Q3: Top 5 customers by lifetime value
SELECT
    customer_id,
    customer_name,
    COUNT(*)                          AS total_orders,
    SUM(quantity)                     AS total_units,
    ROUND(SUM(net_revenue), 2)        AS lifetime_value,
    ROUND(AVG(net_revenue), 2)        AS avg_order_value,
    MIN(order_date)                   AS first_order,
    MAX(order_date)                   AS last_order
FROM sales
WHERE status = 'completed'
GROUP BY customer_id, customer_name
ORDER BY lifetime_value DESC
LIMIT 5;

-- Q4: Monthly revenue trend with MoM growth
WITH monthly AS (
    SELECT
        order_year,
        order_month,
        ROUND(SUM(net_revenue), 2)  AS revenue
    FROM sales
    GROUP BY order_year, order_month
)
SELECT
    order_year,
    order_month,
    revenue,
    LAG(revenue) OVER (ORDER BY order_year, order_month) AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY order_year, order_month))
        / LAG(revenue) OVER (ORDER BY order_year, order_month) * 100,
        1
    ) AS mom_growth_pct
FROM monthly
ORDER BY order_year, order_month;

-- Q5: Salesperson performance scorecard
SELECT
    salesperson,
    COUNT(*)                          AS total_deals,
    COUNT(CASE WHEN status='completed' THEN 1 END) AS won,
    COUNT(CASE WHEN status='refunded' THEN 1 END)  AS refunded,
    ROUND(SUM(net_revenue), 2)        AS total_revenue,
    ROUND(AVG(net_revenue), 2)        AS avg_deal_size,
    ROUND(SUM(gross_profit), 2)       AS total_profit,
    region
FROM sales
GROUP BY salesperson, region
ORDER BY total_revenue DESC;

-- Q6: Product performance analysis (use Parquet partition pruning)
SELECT
    product_id,
    product_name,
    category,
    COUNT(*)                          AS orders,
    SUM(quantity)                     AS units_sold,
    ROUND(AVG(unit_price), 2)         AS avg_unit_price,
    ROUND(SUM(net_revenue), 2)        AS revenue
FROM sales
WHERE order_year = 2024           -- Partition filter: reads only 2024 data
  AND order_month BETWEEN 1 AND 3  -- Partition filter: Q1 only
GROUP BY product_id, product_name, category
ORDER BY revenue DESC;

-- Q7: Regional analysis
SELECT
    region,
    COUNT(DISTINCT customer_id)       AS customers,
    COUNT(*)                          AS orders,
    ROUND(SUM(net_revenue), 2)        AS revenue,
    ROUND(AVG(net_revenue), 2)        AS avg_order_value
FROM sales
GROUP BY region
ORDER BY revenue DESC;

-- Q8: Revenue tier analysis
SELECT
    revenue_tier,
    COUNT(*)                          AS orders,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders,
    ROUND(SUM(net_revenue), 2)        AS revenue,
    ROUND(SUM(net_revenue) * 100.0 / SUM(SUM(net_revenue)) OVER(), 1) AS pct_of_revenue
FROM sales
GROUP BY revenue_tier
ORDER BY revenue DESC;
