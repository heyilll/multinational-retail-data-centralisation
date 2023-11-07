from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
import tabula
import json
import requests
import boto3

class DataExtractor:
    
    # def read_rds_table(self, databaseconnector, tablename):
    #     databaseconnector = DatabaseConnector().init_db_engine()
    #     with databaseconnector.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
    #         result = pd.read_sql_table(tablename, databaseconnector)
    #         return result
        
    def read_rds_table(self, db_connector, table_name):
        db_connector = DatabaseConnector()
        df = pd.read_sql_table(table_name,con=db_connector.init_db_engine('db_creds.yaml'))
        return df    
        
    def retrieve_pdf_data(self, link):
        dataf = tabula.read_pdf(link, pages='all')
        return pd.concat(dataf)
    
    def list_number_of_stores(self, link, dict):
        response = requests.get(link, headers=dict) 
        return response.text
    
    def retrieve_stores_data(self, link: str, dict):    
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
        s3 = boto3.resource('s3')
        cutlink = link.replace('s3://', '')
        cutlink = cutlink.split('/', 1)
        bucket = cutlink[0]
        filename = cutlink[1]
        s3.Bucket(bucket).download_file(filename, filename)
        df = pd.read_csv(filename)
        return df
    
    def extract_json_from_s3(self, link):
        s3 = boto3.resource('s3')
        cutlink = link.replace('s3://', '')
        cutlink = cutlink.split('/', 1)
        bucket = cutlink[0]
        filename = cutlink[1]
        s3.Bucket(bucket).download_file(filename, filename)
        df = pd.read_json(filename) 
        return df
    
if __name__ == "__main__":
    data_extractor = DataExtractor() 
    datacleaner = DataCleaning() 
    dbc = DatabaseConnector()
    
    # df = data_extractor.read_rds_table(dbc, 'legacy_users') 
    # df = datacleaner.clean_user_data(df)  
    # dbc.upload_to_db(df, 'dim_users', 'ldb.yaml')  
    
    # df = data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf') 
    # df = datacleaner.clean_card_data(df)    
    # dbc.upload_to_db(df, 'dim_card_details', 'ldb.yaml') 
     
    # store_dict = { 'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'} 
    #data_extractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', store_dict)
    # df = data_extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', store_dict) 
    # df = pd.read_csv('dim_store_details.csv')
    # df = datacleaner.clean_store_data(df) 
    # dbc.upload_to_db(df, 'dim_store_details', 'ldb.yaml') 
    
    # df = data_extractor.extract_from_s3('s3://data-handling-public/products.csv') 
    # df = pd.read_csv('products.csv')
    # df = datacleaner.convert_product_weights(df) 
    # df = datacleaner.clean_products_data(df)    
    # dbc.upload_to_db(df, 'dim_products', 'ldb.yaml')  
    
    # df = data_extractor.read_rds_table(dbc, 'orders_table')  
    # df = datacleaner.clean_orders_data(df)  
    # dbc.upload_to_db(df, 'orders_table', 'ldb.yaml')
    
    # df = data_extractor.extract_json_from_s3('s3://data-handling-public/date_details.json')
    # df = datacleaner.clean_date_data(df)  
    # dbc.upload_to_db(df, 'dim_date_times', 'ldb.yaml')