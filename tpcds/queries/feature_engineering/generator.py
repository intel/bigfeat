#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# TODO: Create a graph that capture tpcds schema. Nodes: Tables; Edges: Tables are joinable; 
#       Edge value: join key

# TODO: Add Pyhive and SQLAlchemy to the requirements
# TODO: Add filtering of columns based on input from the user
# TODO: Make a plan to understand how to do joins
# TODO: Create a dictionary of columns to idx.


# In[ ]:


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



# In[ ]:


"""
Input
    tables -- List of tables to generate feature maps from e.g., [web_sales,customer]
    col_idx -- Optional: idx of columns corresponding to both tables e.g., [[0,1,2],[2,3,4]].
                If none, use all columns in the feature map. 
Returns a SQL query as a string to generate dense feature maps
"""

def generate_dense_feature_map_query(db,
                                     tables, 
                                     columns=None):
    
    """
    Check if table exists in columns
    """
    
    db.query("show tables")
    result = db.get_result()
    
    for table in tables:

        if table not in list(result['Table']):
            print(f'Table {table} does not exist!')
            return -1

    """
    Get list of columns from all tables
    """
    
    columns = []

    for table in tables:

        sql = f'SHOW columns FROM {table}'
        db.query(sql)
        columns.append(db.get_result())

    all_columns = pd.concat(columns)

    """
    Filter out all columns that are foreign keys
    """

    all_columns = all_columns[~all_columns['Column'].str.contains('sk')]

    # WASAY: Hash function goes here
    print(all_columns)


    """
    Create list of idx and column names to build the map from
    """

    # WASAY: Replace the name with the hash output

    list_of_columns = list(all_columns['Column'])
    list_of_idx = [str(i) for i in range(len(list_of_columns))]

    """
    Create different subsets of the query
    """
    create_query = 'df'
    map_query = f'MAP(ARRAY[{",".join(list_of_idx)}],ARRAY[{",".join(list_of_columns)}])'
    from_query = f'{",".join(tables)}'
    where_query = ''

    query = f'CREATE TABLE {create_query} AS SELECT {map_query} FROM {from_query} WHERE {where_query};'
    print(format_sql(query))


# In[ ]:


generate_dense_feature_map_query(db=db, tables=['customer','web_returns','web_sales'])


# 

# 
