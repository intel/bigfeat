-- Example 1: Get a map of the 10 recent item_sk every customer interacted with

SELECT MAP(ARRAY[cl_customer_sk], ARRAY[MAX_BY(cl_item_sk, cl_action_date*86400+cl_action_time, 10)])
    FROM
        click_log_formatted
    GROUP BY
        cl_customer_sk;


-- Example 2: Get a map of the 10 recent item_sk every customer purchased (action = 5)

SELECT MAP(ARRAY[cl_customer_sk], ARRAY[MAX_BY(cl_item_sk, cl_action_date*86400+cl_action_time, 10)])
    FROM
        click_log_formatted
    WHERE 
        cl_action = 5
    GROUP BY
        cl_customer_sk;

-- Example 3: Get a map of the 10 recent item categories every customer purchased (action = 5)

SELECT MAP(ARRAY[cl_customer_sk], ARRAY[MAX_BY(i_category, cl_action_date*86400+cl_action_time, 10)])
    FROM
        click_log_formatted, item
    WHERE 
        cl_action = 5 AND
        cl_item_sk = i_item_sk
    GROUP BY
        cl_customer_sk;

-- Example 4: Get a map of the 10 most expensive item categories every customer purchased (action = 5)

SELECT MAP(ARRAY[cl_customer_sk], ARRAY[MAX_BY(i_category, ws_list_price, 10)])
    FROM
        click_log_formatted, item, web_sales
    WHERE 
        cl_action = 5 AND
        cl_item_sk = i_item_sk AND
        cl_customer_sk = ws_bill_customer_sk
    GROUP BY
        cl_customer_sk;

