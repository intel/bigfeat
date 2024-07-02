-- Get the full trajectory of every customer session

SELECT MAP(ARRAY[cl_session_id], ARRAY[MIN_BY(cl_action, cl_action_date*86400+cl_action_time, 50)])
    FROM
        click_log_formatted
    GROUP BY
        cl_session_id;

-- Get the full trajectory of every customer session leading to a purchase

SELECT MAP(ARRAY[cl_session_id], ARRAY[MIN_BY(cl_action, cl_action_date*86400+cl_action_time, 50)])
    FROM
        click_log_formatted
    WHERE cl_session_id IN  
        (SELECT cl_session_id from click_log_formatted where cl_action = 5)
    GROUP BY
        cl_session_id;

-- Get the full trajectory of every customer session leading to a purchase

SELECT MAP(ARRAY[cl_session_id], ARRAY[MIN_BY(cl_action, cl_action_date*86400+cl_action_time, 50)])
    FROM
        click_log_formatted
    WHERE cl_session_id NOT IN  
        (SELECT cl_session_id from click_log_formatted where cl_action = 5)
    GROUP BY
        cl_session_id;

-- -- Get the full trajectory of every customer session

-- SELECT MAP(ARRAY[CAST (cl_session_id AS VARCHAR) ||'_'|| CAST(cl_customer_sk AS VARCHAR) ||'_'|| CAST(cl_item_sk AS VARCHAR) ], ARRAY[MIN_BY(cl_action, cl_action_date*86400+cl_action_time, 50)])
--     FROM
--         click_log_formatted
--     GROUP BY
--         cl_session_id, cl_customer_sk, cl_item_sk;


-- -- Get the full trajectory of every customer_session_item that leads to a purchase

-- SELECT MAP(ARRAY[CAST (cl_session_id AS VARCHAR) ||'_'|| CAST(cl_customer_sk AS VARCHAR) ||'_'|| CAST(cl_item_sk AS VARCHAR)], ARRAY[MIN_BY(cl_action, cl_action_date*86400+cl_action_time, 50)])
--     FROM
--         click_log_formatted
--      WHERE
--        CAST (cl_session_id AS VARCHAR) ||'_'|| CAST(cl_customer_sk AS VARCHAR) ||'_'|| CAST(cl_item_sk AS VARCHAR) IN (SELECT CAST (cl_session_id AS VARCHAR) ||'_'|| CAST(cl_customer_sk AS VARCHAR) ||'_'|| CAST(cl_item_sk AS VARCHAR) FROM click_log_formatted WHERE cl_action = 5) 
--     GROUP BY
--         cl_session_id, cl_customer_sk, cl_item_sk;

-- -- Get the full trajectory of every customer_session_item that does not lead to a purchase

-- SELECT MAP(ARRAY[CAST (cl_session_id AS VARCHAR) ||'_'|| CAST(cl_customer_sk AS VARCHAR) ||'_'|| CAST(cl_item_sk AS VARCHAR)], ARRAY[MIN_BY(cl_action, cl_action_date*86400+cl_action_time, 50)])
--     FROM
--         click_log_formatted
--      WHERE
--        CAST (cl_session_id AS VARCHAR) ||'_'|| CAST(cl_customer_sk AS VARCHAR) ||'_'|| CAST(cl_item_sk AS VARCHAR) NOT IN (SELECT CAST (cl_session_id AS VARCHAR) ||'_'|| CAST(cl_customer_sk AS VARCHAR) ||'_'|| CAST(cl_item_sk AS VARCHAR) FROM click_log_formatted WHERE cl_action = 5) 
--     GROUP BY
--         cl_session_id, cl_customer_sk, cl_item_sk;