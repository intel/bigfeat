#!/usr/bin/env python

'''
Click Log Schema: date, time, cust_sk, item_sk, action 
Action types currently supported: view(1) -> add(3) -> purchase(5)
Non-conversion class action: view(1) -> some % of views will go to add (3)
'''

import numpy as np
import random
from enum import Enum
from database import DB
from sql_formatter.core import format_sql
import concurrent.futures
from io import BytesIO
import pandas as pd
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq
import os
import argparse

'''
Enum Section
'''
# Enum for different user actions
class Action(Enum):
    VIEW_ITEM        = 1
    ADD_TO_CART      = 3
    REMOVE_FROM_CART = 4
    PURCHASE         = 5

# Enum for different file formats supported
class FileFormat(Enum):
    CSV      = 'csv'
    PARQUET  = 'parquet'

# Enum for status of Minio file upload
class MinioStatus(Enum):
    SUCCESS         =  0
    CONN_FAILURE    = -1
    BUCKET_MISSING  = -2
    CSV_FAILURE     = -3
    PARQUET_FAILURE = -4
    INVALID_FORMAT  = -5

'''
Global Variables
'''
# TPC-DS uses 0-86399 as time_sk
secs_per_day = 86400


'''
Click Log Config Parameter Class
'''
class ClickLogConfig:

    attributes =  { 'cl_customer_sk' : 'VARCHAR',
                    'cl_item_sk'     : 'VARCHAR',
                    'cl_action'      : 'VARCHAR',
                    'cl_action_time' : 'VARCHAR',
                    'cl_action_date' : 'VARCHAR'
                  }

    partition_key = list(attributes.keys())[-1]

    def __init__(   self,
                    minio_bucket,
                    schema_name,
                    table_name,
                    max_workers                     = 8,
                    session_timeout_secs            = 1800,
                    trajectory_conversion_pct       = 2.08,
                    view_to_purchase_min_delay_secs = 30,
                    view_to_add_min_delay_secs      = 15,
                    add_to_remove_min_delay_secs    = 5,
                    add_to_remove_max_delay_secs    = 15,
                    view_to_add_conversion_pct      = 10,
                    file_format                     = FileFormat.PARQUET.value
                ):
        # Minio bucket
        self.minio_bucket = minio_bucket

        # Name of the schema
        self.schema_name = schema_name

        # Name of the click log table
        self.table_name  = table_name

        # Max number of parallel workers
        self.max_workers = max_workers

        # session timeout is pegged to 30 mins
        # so view, add, purchase have to happen within a 30 mins window
        self.session_timeout_secs = session_timeout_secs

        # Percent of trajectories that convert into purchases
        # 2.08% derived from https://doi.org/10.1038/s41598-020-73622-y
        self.trajectory_conversion_pct = trajectory_conversion_pct

        # Assume 30 secs min. gap between logging of view action and purchase action
        self.view_to_purchase_min_delay_secs = view_to_purchase_min_delay_secs

        # Assume 15 secs min. gap between logging of view action and add action
        self.view_to_add_min_delay_secs = view_to_add_min_delay_secs

        # Assume 5 secs min gap between logging of add to cart and remove from cart actions
        self.add_to_remove_min_delay_secs = add_to_remove_min_delay_secs

        # Assume 15 secs max gap between logging of add to cart and remove from cart actions
        self.add_to_remove_max_delay_secs = add_to_remove_max_delay_secs

        # Assume only 10% of views are added to cart
        self.view_to_add_conversion_pct = view_to_add_conversion_pct

        # Set file format default to parquet
        self.file_format = file_format


'''
Get the record of web sales
'''
def web_sales(db, start_date_sk, start_time_sk, end_date_sk, end_time_sk):
    sql = f"""SELECT ws_item_sk, ws_sold_date_sk, ws_sold_time_sk, ws_bill_customer_sk
              FROM web_sales
              WHERE ws_sold_date_sk BETWEEN {start_date_sk} AND {end_date_sk} AND
                    ws_sold_time_sk BETWEEN {start_time_sk} AND {end_time_sk}
           """
    db.query(sql)
    return db.get_result()

'''
Get the list of items
'''
def items(db):
    sql = f'SELECT i_item_sk FROM item'
    db.query(sql)
    return db.get_result()

'''
Get the list of customers
'''
def customers(db):
    sql = f'SELECT c_customer_sk FROM customer'
    db.query(sql)
    return db.get_result()

'''
Subtract delta secs from date-time for generating log entries
'''
def subtract_time_delta(date_sk, time_sk, delta, config):
    if time_sk < delta:
        return date_sk - 1, secs_per_day - delta + time_sk
    return date_sk, time_sk - delta

'''
Add delta secs to date-time for generating log entries
'''
def add_time_delta(date_sk, time_sk, delta, config):
    new_time_sk = time_sk + delta
    if new_time_sk >= secs_per_day:
        return date_sk + 1, new_time_sk % secs_per_day
    return date_sk, new_time_sk

'''
Insert logs to table
Return : errno, errmsg
Error numbers (errno): MinioStatus enum
'''
def insert_logs(config, logs, date_sk, stime_sk, etime_sk):
    filtered_logs  = [x for x in logs if int(x[4]) == date_sk and
                                         int(x[3]) >= stime_sk and
                                         int(x[3]) <= etime_sk]
    df = pd.DataFrame(filtered_logs, columns = list(config.attributes.keys()))

    # MinIO upload
    client = Minio('localhost:9000', access_key='minio', secret_key='minio123', secure=False)
    if not client:
        errmsg = f'Invalid minio client object. Could not create partition for {date_sk}'
        return MinioStatus.CONN_FAILURE.value, errmsg

    # Check to make sure bucket exist.
    found = client.bucket_exists(config.minio_bucket)
    if not found:
        errmsg = f'Bucket \'{config.minio_bucket}\' does not exist'
        return MinioStatus.BUCKET_MISSING.value, errmsg

    msg = ''
    file_name = config.partition_key + '=' + str(date_sk)
    # if file format is CSV
    if config.file_format == FileFormat.CSV.value:
        csv_bytes = df.to_csv(header = None, index = False).encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)

        # Upload a new minio file
        result = client.put_object(config.minio_bucket,
                                   config.table_name + '/' + file_name,
                                   data = csv_buffer, length = len(csv_bytes),
                                   content_type = 'application/csv')
        if not result:
            errmsg = f'Failed to upload csv for {date_sk} data'
            return MinioStatus.CSV_FAILURE.value, errmsg
        msg = f'Uploaded \'{file_name}\' to \'{config.minio_bucket}/{config.table_name}/\''

    # Else if file format is parquet (snappy)
    elif config.file_format == FileFormat.PARQUET.value:
        df.to_parquet(file_name, engine = 'pyarrow', compression = 'snappy', index = False)

        # Upload the file, renaming it in the process
        result = client.fput_object(config.minio_bucket, config.table_name + '/' + file_name, file_name)
        os.remove(file_name)
        if not result:
            errmsg = f'Failed to upload parquet file for {date_sk} data'
            return MinioStatus.PARQUET_FAILURE.value, errmsg
        msg = f'Uploaded \'{file_name}\' to \'{config.minio_bucket}/{config.table_name}/\''

    # Else if file format is invalid
    else:
        errmsg = f'Invalid file format \'{config.file_format}\''
        return MinioStatus.INVALID_FORMAT.value, errmsg

    return MinioStatus.SUCCESS.value, msg

'''
Generate Conversion Class Trajectory Logs. These end in purchase.
'''
def gen_conversion_logs(db, config, df_web_sales):
    logs = []
    for indx in df_web_sales.index:
        customer_sk      = df_web_sales['ws_bill_customer_sk'][indx].astype(int)
        item_sk          = df_web_sales['ws_item_sk'][indx].astype(int)
        purchase_date_sk = df_web_sales['ws_sold_date_sk'][indx].astype(int)
        purchase_time_sk = df_web_sales['ws_sold_time_sk'][indx].astype(int)

        # generate action logs for view, add and purchase. modify time for view and add
        view_delta = random.randint(config.view_to_purchase_min_delay_secs, config.session_timeout_secs)
        view_date_sk, view_time_sk = subtract_time_delta(purchase_date_sk, purchase_time_sk, view_delta, config)
        view_log = [customer_sk, item_sk, Action.VIEW_ITEM.value, view_time_sk, view_date_sk]
        logs.append([str(x) for x in view_log])

        # Add to cart delay after view is logged and before purchase
        add_delta = random.randint(config.view_to_add_min_delay_secs, view_delta)
        add_date_sk, add_time_sk = add_time_delta(view_date_sk, view_time_sk, add_delta, config)
        add_log = [customer_sk, item_sk, Action.ADD_TO_CART.value, add_time_sk, add_date_sk]
        logs.append([str(x) for x in add_log])
        purchase_log = [customer_sk, item_sk, Action.PURCHASE.value, purchase_time_sk, purchase_date_sk]
        logs.append([str(x) for x in purchase_log])

    return logs

'''
Generate Non-Conversion Class Trajectory Logs. These do not end in purchase.
'''
def gen_non_conversion_logs(db, config, all_customers, all_items, trajectory_cnt,
                            start_date_sk, start_time_sk, end_date_sk, end_time_sk):
    logs = []
    for indx in range(trajectory_cnt):
        # generate action logs for view and add actions. Do not create purchase log.
        view_date_sk = random.randint(start_date_sk, end_date_sk)

        view_time_sk = random.randint(0, secs_per_day-1)
        if view_date_sk == end_date_sk:
            view_time_sk = random.randint(0, end_time_sk)
        elif view_date_sk == start_date_sk:
            view_time_sk = random.randint(start_time_sk, secs_per_day-1)

        customer_sk  = random.choice(all_customers)
        item_sk      = random.choice(all_items)
        view_log     = [customer_sk, item_sk, Action.VIEW_ITEM.value, view_time_sk, view_date_sk]
        logs.append([str(x) for x in view_log])

        # If customer adds item to cart
        if random.randrange(100) <= config.view_to_add_conversion_pct:
            # Add to cart delay after view is logged and before removing from cart
            add_delta = random.randint(config.view_to_add_min_delay_secs, config.session_timeout_secs)
            add_date_sk, add_time_sk = add_time_delta(view_date_sk, view_time_sk, add_delta, config)
            add_log = [customer_sk, item_sk, Action.ADD_TO_CART.value, add_time_sk, add_date_sk]
            logs.append([str(x) for x in add_log])

            # Remove item from cart
            remove_delta = random.randint(config.add_to_remove_min_delay_secs, config.add_to_remove_max_delay_secs)
            remove_date_sk, remove_time_sk = add_time_delta(add_date_sk, add_time_sk, remove_delta, config)
            remove_log = [customer_sk, item_sk, Action.REMOVE_FROM_CART.value, remove_time_sk, remove_date_sk]
            logs.append([str(x) for x in remove_log])

    return logs

'''
Generate Click Log Trajectory Logs
Start and end date, times are inclusive
'''
def gen_click_log(db, config, start_date_sk, start_time_sk, end_date_sk, end_time_sk):
    df_ws = web_sales(db, start_date_sk, start_time_sk, end_date_sk, end_time_sk)
    # Filter out 'NaN' entries
    df_ws = df_ws.dropna(subset = ['ws_bill_customer_sk', 'ws_item_sk', 'ws_sold_date_sk', 'ws_sold_time_sk'])
    items_sold = df_ws['ws_item_sk']
    c_logs = gen_conversion_logs(db, config, df_ws)
    print(f'Conversion Click-Log Entries     = {len(c_logs)}')

    df_i = items(db)
    all_items = df_i['i_item_sk']
    df_c = customers(db)
    all_customers = df_c['c_customer_sk']

    # Conversion trajectory % used to calculate non-conversion trajectory count
    nc_trajectory_cnt = int( 100 / config.trajectory_conversion_pct - 1 ) * len(items_sold)
    nc_logs = gen_non_conversion_logs(  db, config, all_customers, all_items, nc_trajectory_cnt,
                                        start_date_sk, start_time_sk, end_date_sk, end_time_sk  )
    print(f'Non-Conversion Click-Log Entries = {len(nc_logs)}')

    logs = [*c_logs,*nc_logs]
    logs.sort(key = lambda row: (row[3], row[4]))
    print(f'Total Click-Log Entries          = {len(logs)}')

    # Create the list of days, start time and end time
    day_list       = range(start_date_sk, end_date_sk + 1)
    stime_list     = [0] * len(day_list)
    stime_list[0]  = start_time_sk
    etime_list     = [secs_per_day - 1] * len(day_list)
    etime_list[-1] = end_time_sk

    queries = ''
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = [executor.submit(
            insert_logs, config, logs, day_list[x], stime_list[x], etime_list[x])
                                                            for x in range(0, len(day_list))]
        for future in concurrent.futures.as_completed(futures):
            errno, msg = future.result()
            if errno != MinioStatus.SUCCESS.value :
                print(f'ERROR : {msg}. Status: {errno}')
                return False
            # print(f'INFO  : {msg}')

    return True

'''
Create Click Log Table
'''
def create_db_table(db, config, run_query = False):
    fconfig = ''
    if config.file_format == FileFormat.CSV.value:
        fconfig = f'csv_separator = \',\', format = \'{config.file_format}\''
    elif config.file_format == FileFormat.PARQUET.value:
        fconfig = f'format = \'{config.file_format}\''
    else:
        return 'ERROR: Invalid file format \'{config.file_format.value}\''

    # Drop table if already exists
    script_dir = os.path.dirname(os.path.realpath(__file__))
    sql = f'DROP TABLE IF EXISTS hive.{config.schema_name}.{config.table_name}'
    with open(os.path.join(script_dir, 'queries', 'drop_click_log_table.sql'), 'w') as f:
        f.write(format_sql(sql) + ';\n')
    if run_query:
        db.query(sql)

    # Staging table needs all columns to be varchar
    sql  = f'CREATE TABLE IF NOT EXISTS hive.{config.schema_name}.{config.table_name} ('
    sql += ','.join([key + ' ' + config.attributes[key] for key in config.attributes])
    sql += f') WITH (external_location = \'s3a://{config.minio_bucket}/{config.table_name}\', {fconfig})'
    with open(os.path.join(script_dir, 'queries', 'create_click_log_table.sql'), 'w') as f:
        f.write(format_sql(sql) + ';\n')
    if run_query:
        db.query(sql, direct = True)

'''
Click Log Generator
'''
def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("-b", "--bucket", dest = "bucket", default = "tpcds-sf1-partitioned-dsdgen-parquet",
                                                                                help = "Minio Bucket Name")
    parser.add_argument("-s", "--schema", dest = "schema", default = "tpcds_sf1_parquet",
                                                                                help = "TPC-DS Schema Name")
    parser.add_argument("-t", "--table", dest = "table", default = "click_log", help = "Name of the Click Log Table")
    parser.add_argument("-w", "--workers", dest = "workers", default = 8, help = "Num parallel workers", type = int)

    parser.add_argument("-ds", "--start-date", dest = "start_date", default = 2450815, help = "Start Date", type = int)
    parser.add_argument("-de", "--end-date"  , dest = "end_date"  , default = 2450820, help = "End Date"  , type = int)
    parser.add_argument("-ts", "--start-time", dest = "start_time", default = 0      , help = "Start Time", type = int)
    parser.add_argument("-te", "--stop-time" , dest = "end_time"  , default = 86399  , help = "End Time"  , type = int)

    args = parser.parse_args()

    db = DB()

    # Params (in order):
    # => minio bucket name
    # => schema name
    # => click_log table name
    # => max parallel workers
    config = ClickLogConfig(    minio_bucket = args.bucket,
                                schema_name  = args.schema,
                                table_name   = args.table,
                                max_workers  = args.workers,
                                file_format  = FileFormat.PARQUET.value
                            )

    # Create click log history for one month in TPC-DS
    # Date and time sk limits are inclusive
    # Upload to minio
    if not gen_click_log(db, config, args.start_date, args.start_time, args.end_date, args.end_time):
        print("Failed to generate click log and upload it")
        return exit()

    # Create the click log table
    create_db_table(db, config, run_query = False)


if __name__ == "__main__":
    main()

