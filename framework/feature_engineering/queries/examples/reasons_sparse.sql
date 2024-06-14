USE hive.tpcds_sf1_parquet;
-- Compiling resons why ecommerce customers returned their order
        --wr_refunded_customer_sk,
	--array_agg(r_reason_desc) excuses
DROP TABLE return_why;
CREATE TABLE return_why AS
    SELECT
        MULTIMAP_AGG(
            CAST(wr_refunded_customer_sk as DECIMAL(38, 0)),
	    CAST(from_big_endian_64(xxhash64(to_utf8(CAST(r_reason_desc as VARCHAR)))) as DECIMAL(38, 0))
        )
    FROM
        web_sales, web_returns, reason
    WHERE
        ws_ship_customer_sk = wr_refunded_customer_sk AND
        r_reason_sk = wr_reason_sk
    GROUP BY
        wr_refunded_customer_sk;