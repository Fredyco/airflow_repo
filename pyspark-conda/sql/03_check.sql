-- Langkah 3: cek hasil load (jalankan di pgAdmin pada database "dwh").

-- Jumlah baris tiap tabel
SELECT 'dim_customer' AS tabel, COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_product',  COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL SELECT 'dim_date',     COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_sales',   COUNT(*) FROM fact_sales;

-- Total penjualan per region & kategori (join fact ke dimension)
SELECT l.region, p.category, ROUND(SUM(f.sales), 2) AS total_sales
FROM fact_sales f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_product  p ON f.product_id  = p.product_id
GROUP BY l.region, p.category
ORDER BY l.region, p.category;

-- Penjualan per tahun & kuartal
SELECT d.year, d.quarter, ROUND(SUM(f.sales), 2) AS total_sales
FROM fact_sales f
JOIN dim_date d ON f.order_date_id = d.date_id
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;
