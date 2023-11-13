from sqlalchemy import create_engine, inspect
import yaml

class DatabaseConnector:
    """
    Contains methods that are used to connect with and upload data to the database.
    """ 
    def read_db_creds(self, inf):
        '''
        Reads the credentials from a yaml file and return a dictionary of the credentials

                Parameters:
                        inf (str): File path to credentials YAML file

                Returns:
                        dbcreds (dict): A dictionary of the credentials
        ''' 
        with open(inf, 'r') as f:
            dbcreds = yaml.safe_load(f)
        return dbcreds 
    
    def init_db_engine(self, inf):
        '''
        Reads the credentials from the return of read_db_creds and initialises and returns an SQLalchemy database engine.

                Parameters:
                        inf (dict): A dictionary of credentials

                Returns:
                        engine (str): A SQLalchemy engine
        ''' 
        creds = self.read_db_creds(inf)
        engine = create_engine(f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine    
    
    def list_db_tables(self):
        '''
        Lists all the tables in the database to show which tables data can be extracted from.

                Parameters:
                        N/A

                Returns:
                        engine (str): A SQLalchemy engine
        ''' 
        engine = self.init_db_engine('db_creds.yaml')
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables   
    
    def upload_to_db(self, dataf, tablename, inf):  
        '''
        Takes in a Pandas DataFrame and the target table name to upload to in the database. A credentials YAML file is also passed to the method in order to 
        call the init_db_engine() to create an engine for the to_sql() method. The credentials file can be changed to upload to a local or online database.

                Parameters:
                        dataf (pd.Dataframe): A pandas Dataframe
                        tablename (str): Table name
                        inf (dict) : Filepath to the credentials YAML file

                Returns:
                        N/A 
        ''' 
        dataf.to_sql(tablename, self.init_db_engine(inf), if_exists='replace')   