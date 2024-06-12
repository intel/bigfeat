USE hive.tpcds_sf1_parquet;

DROP TABLE sparse_cl_1;
DROP TABLE sparse_cl_2;
DROP TABLE sparse_cl_3;

-- Get trajectory of every customer,item pair

CREATE TABLE sparse_cl_1 AS 
    SELECT
        MULTIMAP_AGG(
            CAST(cl_customer_sk as DECIMAL(38, 0)),
	    CAST( cl_action as DECIMAL(38, 0))
        )
    AS 
        trajectory
    FROM
        click_log
    GROUP BY
        cl_customer_sk,cl_item_sk;


-- Get trajectory of every customer that leads to a purchase

CREATE TABLE sparse_cl_2 AS 
    SELECT
        MULTIMAP_AGG(
            CAST(cl_customer_sk as DECIMAL(38, 0)),
	    CAST( cl_action as DECIMAL(38, 0))
        )
    AS 
        conversion_trajectory
    FROM
        click_log
    WHERE
        cl_customer_sk IN (SELECT cl_customer_sk FROM click_log WHERE cl_action = '5') 
        AND
        cl_item_sk IN (SELECT cl_item_sk FROM click_log WHERE cl_action = '5')
    GROUP BY
        cl_customer_sk,cl_item_sk;

-- Get trajectories that don't lead to a purchase

CREATE TABLE sparse_cl_3 AS 
    SELECT
        cl_action_date, MULTIMAP_AGG(
            CAST(cl_customer_sk as DECIMAL(38, 0)),
	    CAST( cl_action as DECIMAL(38, 0))
        )
    AS 
        non_conversion_trajectory
    FROM
        click_log
    WHERE
        cl_customer_sk IN (SELECT cl_customer_sk FROM click_log WHERE cl_action != '5') 
        AND
        cl_item_sk IN (SELECT cl_item_sk FROM click_log WHERE cl_action != '5')
    GROUP BY
        cl_customer_sk,cl_item_sk,cl_action_date
    ORDER BY
        cl_action_date;
