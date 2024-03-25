#!/usr/bin/env python

# TODO: Add Pyhive and SQLAlchemy to the requirements
# TODO: Create a dictionary of columns to idx.

from database import DB
from sql_formatter.core import format_sql

"""
Input
    column    -- contains name and type of the column
    precision -- total number of digits (default 18)
    scale     -- number of digits in the fractional part (default 0)
Returns a SQL function as a string to convert column data type to fixed precision
"""
def convert_col_type_to_decimal(column, precision=38, scale=0):
    cols = column.split()
    if len(cols) != 2:
        print("Format incorrect: {column}. Aborting.")
        sys.exit()

    if any(x in cols[1] for x in ['boolean', 'int', 'real', 'double', 'decimal']):
        query = f'CAST({cols[0]} AS DECIMAL({precision},{scale}))'

    elif any(x in cols[1] for x in ['char', 'varbinary', 'uuid']):
        query = f'CAST(from_big_endian_64(xxhash64(to_utf8(\
                       CAST({cols[0]} AS VARCHAR)))) AS DECIMAL({precision},{scale}))'
    else:
        print("Can't support FP conversion for {column}. Aborting.")
        sys.exit()

    return query


"""
Input
    op    -- SQL operation type - 'AND', 'OR'
    attrs -- list of join attributes (default is None)
Returns a SQL WHERE clause as a string
"""
def sql_op(op, attrs=None):
    if not attrs:
        return ''
    elif len(attrs) == 1:
        return attrs[0]

    clause = '(' + attrs[0]
    for attr in attrs[1:]:
        clause += ' ' + op + ' ' + attr
    return clause + ')'


"""
Input
    db -- An object of the class DB
    tables -- List of tables to generate feature maps from e.g., [web_sales,customer]
    feature_map_table_name -- Name of the feature map table
    feature_map_column_name -- Name of the feature map column name
    mode -- Create or Alter
    col_idx -- Optional: idx of columns corresponding to both tables e.g., [[0,1,2],[2,3,4]].
                If none, use all columns in the feature map. 
    Where -- The where clause specified as a string            
Returns a SQL query as a string to generate dense feature maps
"""
def generate_dense_feature_map_query(db,
                                     tables,
                                     feature_map_table_name = 'dense',
                                     feature_map_column_name = "test",
                                     mode = "create",
                                     column_ids = None,
                                     where_clause = None):
    """
    Error checks
    """

    # Check that all tables exist
    db.query("show tables")
    result = db.get_result()
    for table in tables:
        if table not in list(result['Table']):
            print(f'Table {table} does not exist!')
            return -1
    
    # Check if len(column_ids) == len(tables) 
    if column_ids != None: 
        if len(column_ids) != len(tables):
            print("Length of column_ids not equal to len of tables")
            return -1
    
    # Check if mode is correctly specifed
    if mode not in ["create","alter"]:
        print(f'mode is not one of {" ".join(mode)}')
        return -1

    """
    Get list of columns from all tables
    """
    columns = []

    if column_ids == None:

        for table in tables:
            sql = f'SHOW columns FROM {table}'
            db.query(sql)
            res = db.get_result().to_dict()
            columns.extend([x + ' ' + y for x,y in zip(res['Column'].values(), res['Type'].values())])
    else:

        for table, column_id in zip(tables, column_ids):
            sql = f'SHOW columns FROM {table}'
            db.query(sql)
            res = db.get_result().iloc[column_id].to_dict()
            columns.extend([x + ' ' + y for x,y in zip(res['Column'].values(), res['Type'].values())])


    """
    Filter out all columns that are foreign keys
    """
    all_columns = [entry for entry in columns if '_sk ' not in entry]

    """
    Generate decimal conversion subquery for all columns
    Implement a hash value for char array and varchar data types
    """
    hashed_columns = []
    for column in all_columns:
        hashed_columns.append(convert_col_type_to_decimal(column))
    
    
    """
    Create list of idx and column names to build the map from
    """
    list_of_idx = [str(i) for i in range(len(hashed_columns))]

    """
    Create different subsets of the query
    """
    
    map_query = f'MAP(ARRAY[{",".join(list_of_idx)}],ARRAY[{",".join(hashed_columns)}])'
    from_query = f'{",".join(tables)}'
    select_query = f"SELECT {map_query} AS {feature_map_column_name} FROM {from_query}"

    if not where_clause:
        where_query = ""
    else:
        where_query = f' WHERE {where_clause}'

    """
    Create the final query based on the mode
    """
    if mode == "create":
        format = "(format='PARQUET')"
        query = f"""CREATE TABLE IF NOT EXISTS 
                    {feature_map_table_name} 
                    WITH {format} 
                    AS {select_query} 
                    {where_query};
                """
    
    # TODO The alter query runs but does not work as expected. Look into it.
    elif mode == "alter":
        type = "map(integer, decimal(38,0))" 
        query = f"""ALTER TABLE IF EXISTS 
                    {feature_map_table_name} 
                    ADD COLUMN IF NOT EXISTS 
                    {feature_map_column_name} 
                    {type};\n
                """
        query += f"""INSERT INTO 
                    {feature_map_table_name} 
                    ({feature_map_column_name}) 
                    {select_query} 
                    {where_query};
                """
        
    print(format_sql(query))
    return query


# Example

cond1  = 'web_sales.ws_bill_customer_sk = customer.c_customer_sk'
cond2a = 'web_sales.ws_bill_customer_sk = web_returns.wr_refunded_customer_sk'
cond2b = 'web_sales.ws_bill_customer_sk = web_returns.wr_returning_customer_sk'
cond2  = sql_op('OR', [cond2a, cond2b])
clause = sql_op('AND', [cond1, cond2])


db= DB()

generate_dense_feature_map_query(db=db,
                                 tables=['customer','web_returns','web_sales'],
                                 column_ids=None,
                                 where_clause=clause, 
                                 feature_map_column_name="test",
                                 feature_map_table_name="fm_1",
                                 mode = "alter")

