#!/usr/bin/env python


# TODO: Create a graph that capture tpcds schema. Nodes: Tables; Edges: Tables are joinable; 
#       Edge value: join key

# TODO: Add Pyhive and SQLAlchemy to the requirements
# TODO: Add filtering of columns based on input from the user
# TODO: Make a plan to understand how to do joins
# TODO: Create a dictionary of columns to idx.


from pyhive import presto
from sqlalchemy import create_engine, text
import pandas as pd
from sql_formatter.core import format_sql


"""
A database class built on top of sqlalchemy
"""
class DB:

    def __init__(self, 
                 database = 'presto',
                 ip = 'localhost', 
                 port = '8080', 
                 catalog = 'hive', 
                 schema ='tpcds_sf1_parquet' ):
        connection_string = f'{database}://{ip}:{port}/{catalog}/{schema}'
        self.engine = create_engine(connection_string)
        self.cursor = self.engine.connect()
    
    def query(self, sql):
        self.result = self.cursor.execute(text(sql))
        return self.result
    
    def get_result(self,
               limit = None,
               format="pd"):
        
        if format == "pd":
            if limit == None:
                return pd.DataFrame(self.result.all())    
            
            return pd.DataFrame(self.result.fetchmany(limit))
        
    
db = DB()
db.query("show columns from customer")
result = db.get_result()
for column in result:
    print(column)


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
    tables -- List of tables to generate feature maps from e.g., [web_sales,customer]
    col_idx -- Optional: idx of columns corresponding to both tables e.g., [[0,1,2],[2,3,4]].
                If none, use all columns in the feature map. 
Returns a SQL query as a string to generate dense feature maps
"""
def generate_dense_feature_map_query(db,
                                     tables, 
                                     columns=None,
                                     where=None):
    """
    Check if table exists in columns
    """
    db.query("show tables")
    result = db.get_result()
    
    for table in tables:
        if table not in list(result['Table']):
            print(f'Table {table} does not exist!')
            return -1

    # Sounak: What happens if columns arg is not empty; not handled in code
    """
    Get list of columns from all tables
    """
    columns = []
    for table in tables:
        sql = f'SHOW columns FROM {table}'
        db.query(sql)
        res = db.get_result().to_dict()
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
    create_query = 'df'
    map_query = f'MAP(ARRAY[{",".join(list_of_idx)}],ARRAY[{",".join(hashed_columns)}])'
    from_query = f'{",".join(tables)}'

    """
    Generate the final query
    """
    # Sounak: CREATE TABLE IF NOT EXISTS {tablename} WITH (format='PARQUET') AS
    query = f'CREATE TABLE {create_query} AS SELECT {map_query} FROM {from_query}'
    if not where:
        query += f';'
    else:
        query += f' WHERE {where};'

    print(format_sql(query))


cond1  = 'web_sales.ws_bill_customer_sk = customer.c_customer_sk'
cond2a = 'web_sales.ws_bill_customer_sk = web_returns.wr_refunded_customer_sk'
cond2b = 'web_sales.ws_bill_customer_sk = web_returns.wr_returning_customer_sk'
cond2  = sql_op('OR', [cond2a, cond2b])
clause = sql_op('AND', [cond1, cond2])

generate_dense_feature_map_query(db=db,
                                 tables=['customer','web_returns','web_sales'],
                                 where=clause)

