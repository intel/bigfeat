CREATE SCHEMA IF NOT EXISTS hive.tpcds_sf1_parquet WITH (location = 's3a://tpcds-sf1-partitioned-dsdgen-parquet/');
CREATE TABLE hive.tpcds_sf1_parquet.call_center WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.call_center;
CREATE TABLE hive.tpcds_sf1_parquet.catalog_page WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.catalog_page;
CREATE TABLE hive.tpcds_sf1_parquet.catalog_returns WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.catalog_returns;
CREATE TABLE hive.tpcds_sf1_parquet.catalog_sales WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.catalog_sales;
CREATE TABLE hive.tpcds_sf1_parquet.customer WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.customer;
CREATE TABLE hive.tpcds_sf1_parquet.customer_address WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.customer_address;
CREATE TABLE hive.tpcds_sf1_parquet.customer_demographics WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.customer_demographics;
CREATE TABLE hive.tpcds_sf1_parquet.date_dim WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.date_dim;
CREATE TABLE hive.tpcds_sf1_parquet.household_demographics WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.household_demographics;
CREATE TABLE hive.tpcds_sf1_parquet.income_band WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.income_band;
CREATE TABLE hive.tpcds_sf1_parquet.item WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.item;
CREATE TABLE hive.tpcds_sf1_parquet.promotion WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.promotion;
CREATE TABLE hive.tpcds_sf1_parquet.reason WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.reason;
CREATE TABLE hive.tpcds_sf1_parquet.ship_mode WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.ship_mode;
CREATE TABLE hive.tpcds_sf1_parquet.store WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.store;
CREATE TABLE hive.tpcds_sf1_parquet.store_returns WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.store_returns;
CREATE TABLE hive.tpcds_sf1_parquet.store_sales WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.store_sales;
CREATE TABLE hive.tpcds_sf1_parquet.time_dim WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.time_dim;
CREATE TABLE hive.tpcds_sf1_parquet.warehouse WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.warehouse;
CREATE TABLE hive.tpcds_sf1_parquet.web_page WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.web_page;
CREATE TABLE hive.tpcds_sf1_parquet.web_returns WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.web_returns;
CREATE TABLE hive.tpcds_sf1_parquet.web_sales WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.web_sales;
CREATE TABLE hive.tpcds_sf1_parquet.web_site WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.web_site;
CREATE TABLE hive.tpcds_sf1_parquet.inventory WITH (format='PARQUET') AS SELECT * FROM tpcds.sf1.inventory;