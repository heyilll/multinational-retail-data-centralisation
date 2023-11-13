from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
import tabula
import json
import requests
import boto3

class DataExtractor:  
    """
    This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
    The methods extracts data from a particular data source, such as CSV files, an API and an S3 bucket.
    """ 
    def read_rds_table(self, db_connector, table_name): 
        '''
        Extracts a target database table to a pandas DataFrame.

                Parameters:
                        db_connector (engine): sqlalchemy engine
                        table_name (str): table name

                Returns:
                        df (pd.Dataframe): pandas DataFrame
        ''' 
        db_connector = DatabaseConnector()
        df = pd.read_sql_table(table_name,con=db_connector.init_db_engine('db_creds.yaml'))
        return df    
        
    def retrieve_pdf_data(self, link):
        '''
        Uses the tabula package to take in a link to a pdf as an argument and returns the pdf data in the form
        of a pandas DataFrame.

                Parameters:
                        link (str): A link to a pdf

                Returns:
                        full_df (pd.Dataframe): pandas DataFrame
        ''' 
        dataf = tabula.read_pdf(link, pages='all')
        full_df = pd.concat(dataf)
        return full_df
    
    def list_number_of_stores(self, link, dict):
        '''
        Returns the number of stores to extract. 
        Takes in the number of stores endpoint and header dictionary as an argument.

                Parameters:
                        link (str): link to api 
                        dict (dict): headers dictionary for API

                Returns:
                        response.text (str): String returned by api
        ''' 
        response = requests.get(link, headers=dict) 
        return response.text
    
    def retrieve_stores_data(self, link: str, dict):    
        '''
        Takes the retrieve a store endpoint as an argument and extracts all the stores from the API, saving them in a pandas DataFrame.
        This method loops through creating a custom API link for each store number and retrieves a pandas dataframe. After all store endpoints
        are retrieved from, it concatenates every dataframe into a single large pandas dataframe and returns this. 

                Parameters:
                        link (str): link to api 
                        dict (dict): Headers dictionary

                Returns:
                        full_df (pd.Dataframe): pandas Dataframe
        ''' 
        list_of_df = []
        maxstores = 451
        
        for store in range(maxstores):
            newlink = link + str(store)
            response = requests.get(newlink, headers = dict)
            response_dict = json.loads(response.text)
            df = pd.DataFrame.from_dict([response_dict]) 
            list_of_df.append(df)
            
        full_df = pd.concat(list_of_df)    
        return full_df
    
    def extract_from_s3(self, link: str):
        '''
        Uses the boto3 package to download and extract the information returning a pandas DataFrame. 
        This method takes a link in the form 's3://bucket/filename' as an argument and return a pandas DataFrame of 
        the s3 object from the link.

                Parameters:
                        link (str): link to API

                Returns:
                        df (pd.Dataframe): pandas Dataframe
        ''' 
        s3 = boto3.resource('s3')
        cutlink = link.replace('s3://', '')
        cutlink = cutlink.split('/', 1)
        bucket = cutlink[0]
        filename = cutlink[1]
        s3.Bucket(bucket).download_file(filename, filename)
        df = pd.read_csv(filename)
        return df
    
    def extract_json_from_s3(self, link):
        '''
        Reads a JSON file from a link containing the details of when each sale happened, as well as related attributes.
        Returns a pandas Dataframe of the JSON data.

                Parameters:
                        link (str): link to API 

                Returns:
                        df (pd.Dataframe): A pandas Dataframe
        ''' 
        s3 = boto3.resource('s3')
        cutlink = link.replace('s3://', '')
        cutlink = cutlink.split('/', 1)
        bucket = cutlink[0]
        filename = cutlink[1]
        s3.Bucket(bucket).download_file(filename, filename)
        df = pd.read_json(filename) 
        return df
    
# The following commands were used for the project milestone tasks.

# if __name__ == "__main__":
    # data_extractor = DataExtractor() 
    # datacleaner = DataCleaning() 
    # dbc = DatabaseConnector()
    
    # df = data_extractor.read_rds_table(dbc, 'legacy_users') 
    # df = datacleaner.clean_user_data(df)  
    # dbc.upload_to_db(df, 'dim_users', 'ldb.yaml')  
    
    # df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf') 
    # df = datacleaner.clean_card_data(df)    
    # dbc.upload_to_db(df, 'dim_card_details', 'ldb.yaml') 
     
    # store_dict = { 'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'} 
    #data_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', store_dict)
    # df = data_extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', store_dict) 
    # df = datacleaner.clean_store_data(df) 
    # dbc.upload_to_db(df, 'dim_store_details', 'ldb.yaml') 
    
    # df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv') 
    # df = datacleaner.convert_product_weights(df) 
    # df = datacleaner.clean_products_data(df)    
    # dbc.upload_to_db(df, 'dim_products', 'ldb.yaml')  
    
    # df = data_extractor.read_rds_table(dbc, 'orders_table')  
    # df = datacleaner.clean_orders_data(df)  
    # dbc.upload_to_db(df, 'orders_table', 'ldb.yaml')
    
    # df = data_extractor.extract_json_from_s3('s3://data-handling-public/date_details.json')
    # df = datacleaner.clean_date_data(df)  
    # dbc.upload_to_db(df, 'dim_date_times', 'ldb.yaml')