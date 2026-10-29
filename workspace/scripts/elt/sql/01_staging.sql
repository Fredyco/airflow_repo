-- STAGING: membersihkan data mentah di schema raw -> schema staging
-- Semua transformasi dilakukan DI DALAM database (inilah "T" pada ELT)
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_sales;
CREATE TABLE staging.stg_sales AS
SELECT DISTINCT ON ("OrderID")                                  -- hapus duplikat OrderID
       "OrderID"                                    AS order_id,
       "OrderDate"::date                            AS order_date,
       "CustomerID"                                 AS customer_id,
       INITCAP(TRIM("Product"))                     AS product,       -- LAPTOP -> Laptop
       "Category"                                   AS category,
       "Quantity"::int                              AS quantity,
       "UnitPrice"::bigint                          AS unit_price,
       "Quantity"::int * "UnitPrice"::bigint        AS total_price,
       COALESCE("PaymentMethod", 'Unknown')         AS payment_method
FROM raw.sales
WHERE "Quantity" IS NOT NULL                                     -- buang baris tanpa quantity
ORDER BY "OrderID";

DROP TABLE IF EXISTS staging.stg_customers;
CREATE TABLE staging.stg_customers AS
SELECT "CustomerID"        AS customer_id,
       "CustomerName"      AS customer_name,
       "City"              AS city,
       "Segment"           AS segment,
       "JoinDate"::date    AS join_date
FROM raw.customers;
