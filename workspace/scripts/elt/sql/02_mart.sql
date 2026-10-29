-- MART: tabel siap pakai untuk analisis/dashboard
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.sales_by_city_month;
CREATE TABLE mart.sales_by_city_month AS
SELECT c.city,
       TO_CHAR(s.order_date, 'YYYY-MM')   AS order_month,
       COUNT(*)                           AS total_orders,
       SUM(s.quantity)                    AS total_qty,
       SUM(s.total_price)                 AS revenue
FROM staging.stg_sales s
JOIN staging.stg_customers c USING (customer_id)
GROUP BY c.city, TO_CHAR(s.order_date, 'YYYY-MM')
ORDER BY order_month, revenue DESC;

DROP TABLE IF EXISTS mart.top_products;
CREATE TABLE mart.top_products AS
SELECT category,
       product,
       SUM(total_price)                                                AS revenue,
       DENSE_RANK() OVER (PARTITION BY category ORDER BY SUM(total_price) DESC) AS rank_in_category
FROM staging.stg_sales
GROUP BY category, product;
