from sqlalchemy import create_engine, inspect
import yaml

class DatabaseConnector:

    def read_db_creds(self, inf):
        with open(inf, 'r') as f:
            dbcreds = yaml.safe_load(f)
        return dbcreds 
    
    def init_db_engine(self, inf):
        creds = self.read_db_creds(inf)
        engine = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine    
    
    def list_db_tables(self):
        engine = self.init_db_engine('db_creds.yaml')
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables   
    
    def upload_to_db(self, dataf, tablename, inf):  
        dataf.to_sql(tablename, self.init_db_engine(inf), if_exists='replace')  
        
if __name__ == "__main__":
    data_c = DatabaseConnector() 
    df = data_c.list_db_tables() 
    print(df)                 