-- Use Databricks SQL or Spark SQL in notebooks
CREATE DATABASE IF NOT EXISTS dw_sales_db;
-- Register tables as managed tables for SQL catalog convenience
CREATE TABLE IF NOT EXISTS dw_sales_db.dim_customer USING DELTA LOCATION '/
mnt/dw_gold/dim_customer';
CREATE TABLE IF NOT EXISTS dw_sales_db.dim_product USING DELTA LOCATION '/
mnt/dw_gold/dim_product';
CREATE TABLE IF NOT EXISTS dw_sales_db.dim_store USING DELTA LOCATION '/mnt/
dw_gold/dim_store';
CREATE TABLE IF NOT EXISTS dw_sales_db.dim_date USING DELTA LOCATION '/mnt/
dw_gold/dim_date';
CREATE TABLE IF NOT EXISTS dw_sales_db.fact_sales USING DELTA LOCATION '/mnt/
dw_gold/fact_sales';
