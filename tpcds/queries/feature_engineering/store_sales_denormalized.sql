CREATE TABLE IF NOT EXISTS store_sales_denorm
WITH (format='PARQUET') AS
(SELECT * FROM store_sales,
               date_dim,
               time_dim,
               customer,
               customer_demographics,
               household_demographics,
               customer_address,
               store,
               promotion,
               item
          WHERE ss_sold_date_sk = d_date_sk
            AND ss_store_sk     = s_store_sk
            AND ss_sold_time_sk = t_time_sk
            AND ss_item_sk      = i_item_sk
            AND ss_customer_sk  = c_customer_sk
            AND ss_cdemo_sk     = cd_demo_sk
            AND ss_hdemo_sk     = hd_demo_sk
            AND ss_addr_sk      = ca_address_sk
            AND ss_promo_sk     = p_promo_sk);
