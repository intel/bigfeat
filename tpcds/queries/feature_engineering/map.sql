USE hive.tpcds_sf1_parquet;
-- All of Keys and the Values have to be of the same type. In this case they all have to be float
-- Create a map with three keys and three values
CREATE TABLE df AS 
    SELECT 
        MAP(ARRAY[1,2,3], 
            ARRAY[
                CAST(from_big_endian_64(xxhash64(to_utf8(CAST(c_customer_id AS VARCHAR)))) AS decimal),
                ws_net_paid,
                ws_net_profit
    ]) AS 
        c_paid_profit
    FROM 
        web_sales, customer
    WHERE
        web_sales.ws_bill_customer_sk = customer.c_customer_sk;

----

-- MAP( <keys>, <values> )
-- MAP (<bigint>, <floats>)
-- select * from df where c_paid_profit[2] = 53.71;
-- What needs to be unique here?
-- What is the physical storage of these maps in memory/disk etc.?