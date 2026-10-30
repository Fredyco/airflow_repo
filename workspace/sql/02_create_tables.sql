-- Langkah 2: buat tabel star schema Superstore.
-- Jalankan di pgAdmin pada database "dwh" (Query Tool).
-- Menjalankan ulang file ini = mengosongkan semua tabel (dibuat ulang dari awal).

DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer, dim_product, dim_location, dim_date;

-- Dimension: siapa (customer)
CREATE TABLE dim_customer (
    customer_id     VARCHAR(20)  PRIMARY KEY,
    customer_name   VARCHAR(100) NOT NULL,
    segment         VARCHAR(20)  NOT NULL
);

-- Dimension: apa (produk)
CREATE TABLE dim_product (
    product_id      VARCHAR(20)  PRIMARY KEY,
    product_name    VARCHAR(255) NOT NULL,
    category        VARCHAR(50)  NOT NULL,
    sub_category    VARCHAR(50)  NOT NULL
);

-- Dimension: di mana (lokasi)
CREATE TABLE dim_location (
    location_id     INT          PRIMARY KEY,
    country_region  VARCHAR(50)  NOT NULL,
    region          VARCHAR(20)  NOT NULL,
    state           VARCHAR(50)  NOT NULL,
    city            VARCHAR(100) NOT NULL,
    postal_code     VARCHAR(10)  NOT NULL
);

-- Dimension: kapan (tanggal)
CREATE TABLE dim_date (
    date_id         INT          PRIMARY KEY,   -- contoh: 20201108
    full_date       DATE         NOT NULL,
    year            INT          NOT NULL,
    quarter         INT          NOT NULL,
    month           INT          NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,
    day_name        VARCHAR(10)  NOT NULL
);

-- Fact: transaksi penjualan, berisi angka + ID yang menunjuk ke dimension
CREATE TABLE fact_sales (
    row_id          INT            PRIMARY KEY,
    order_id        VARCHAR(20)    NOT NULL,
    order_date_id   INT            NOT NULL REFERENCES dim_date (date_id),
    ship_date_id    INT            NOT NULL REFERENCES dim_date (date_id),
    customer_id     VARCHAR(20)    NOT NULL REFERENCES dim_customer (customer_id),
    product_id      VARCHAR(20)    NOT NULL REFERENCES dim_product (product_id),
    location_id     INT            NOT NULL REFERENCES dim_location (location_id),
    ship_mode       VARCHAR(20)    NOT NULL,
    sales           NUMERIC(12, 4) NOT NULL,
    quantity        INT            NOT NULL,
    discount        NUMERIC(4, 2)  NOT NULL,
    profit          NUMERIC(12, 4) NOT NULL
);
