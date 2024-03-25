from sqlalchemy import create_engine, text
import pandas as pd

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
        
    
# db = DB()
# db.query("show columns from customer")
# result = db.get_result()
# for column in result:
#     print(column)

