import pandas as pd
import numpy as np

class DataCleaning:
    
    def clean_user_data(self, ndf: pd.DataFrame):
        df = ndf.copy()
        df.first_name = df.first_name.astype('string')
        df.last_name = df.last_name.astype('string')
        df.email_address = df.email_address.astype('string')
        df.phone_number = df.phone_number.astype('string')
        df.company = df.company.astype('string')
        df.address = df.address.astype('string')
        df.user_uuid = df.user_uuid.astype('string')

        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        df = df.dropna() 
        df['country_code'].replace({'GGB': 'GB'}, inplace = True)
        df.country = df.country.astype('category')
        df.country_code = df.country_code.astype('string')
        return df
    
    def clean_card_data(self, df: pd.DataFrame):
        def removeq(x):
            if '?' in x:
                return x.replace('?', '')
            else:
                return x
            
        newdf = df.copy()
        newdf = newdf.dropna() 
        newdf['date_payment_confirmed'] = pd.to_datetime(newdf['date_payment_confirmed'], errors='coerce')
        newdf = newdf[newdf['date_payment_confirmed'].notnull()]

        df.card_provider = df.card_provider.astype('category')
        newdf.card_number = newdf.card_number.astype('string')
        newdf.card_number = newdf.card_number.apply(removeq)
        newdf.expiry_date = newdf.expiry_date.astype('string')
        return newdf
    
    def clean_store_data(self, df):
        newdf = df.copy()
        
        newdf['opening_date'] = pd.to_datetime(newdf['opening_date'], errors='coerce')
        newdf = newdf[newdf['opening_date'].notnull()]
        
        newdf.store_code = newdf.store_code.astype('string')
        
        newdf.staff_numbers = pd.to_numeric(newdf.staff_numbers, errors='coerce')
        newdf.staff_numbers = newdf.staff_numbers.astype('Int64')
        
        newdf.latitude = newdf.latitude.astype('float')
        newdf.longitude = newdf.longitude.astype('float')
        newdf.locality = newdf.locality.astype('string')
        newdf.address = newdf.address.astype('string')
        newdf.store_type = newdf.store_type.astype('category')
        newdf.country_code = newdf.country_code.astype('category')
        newdf['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'}, inplace = True)
        newdf.continent = newdf.continent.astype('category')
        
        newdf = newdf.drop('lat', axis=1)
        newdf = newdf.drop('level_0', axis=1)
        
        newdf.loc[df["store_type"] == "Web Portal", "longitude"] = np.nan
        newdf.loc[df["store_type"] == "Web Portal", "latitude"] = np.nan
        newdf.loc[df["store_type"] == "Web Portal", "locality"] = np.nan
        newdf.loc[df["store_type"] == "Web Portal", "address"] = np.nan
        return newdf
        
    def convert_product_weights(self, df):
        def func_x(x): 
            if 'x' in x:
                numbers = x.split(' x ')
                if numbers[1][-1] == 'g':
                    numbers[1] = numbers[1][:-1] 
                product = int(numbers[0]) * int(numbers[1]) 
                return str(product) + 'g'
            else: 
                x = x.replace(' .', '')
                x = x.replace('. ', '')
                x = x.replace(' ', '')
                return x
       
        def func_g(x): 
            if x[-2].isdigit():
                return x + 'g'
            else: 
                return x
       
        def func_kg(x): 
            if x[-2:] == 'oz':
                return (float(x[:-2]) * 0.0283495)            
            elif x[-2:] != "kg":
                return float(x[:-2])/1000
            else: 
                return float(x[:-2])
            
        newdf = df.copy()
        newdf['date_added'] = pd.to_datetime(newdf['date_added'], errors='coerce')
        newdf = newdf[newdf['date_added'].notnull()]
        newdf['weight'] = newdf['weight'].astype('string')
        newdf['weight'] = newdf['weight'].apply(func_x)
        newdf['weight'] = newdf['weight'].apply(func_g)
        newdf['weight'] = newdf['weight'].apply(func_kg)
        newdf['weight'] = newdf['weight'].astype('float')
        return newdf
    
    def clean_products_data(self, df): 
        def removep(x):
            if x[0] == '£':
                return x[1:]
            else:
                return x
            
        newdf = df.copy()
        newdf.product_name = newdf.product_name.astype('string')
        newdf.product_code = newdf.product_code.astype('string')
        newdf.EAN = newdf.EAN.astype('string') 
        newdf.category = newdf.category.astype('category')
        newdf.uuid = newdf.uuid.astype('string')
           
        newdf = newdf.rename(columns = {'removed': 'still_available'})   
        newdf.loc[newdf["still_available"] == 'Still_available', "still_available"] = True
        newdf.loc[newdf["still_available"] == "Removed", "still_available"] = False
        newdf.still_available = newdf.still_available.astype('bool')   
        
        newdf['product_price'] = newdf['product_price'].apply(removep)
        newdf['product_price'] = newdf['product_price'].astype('float')
        
        mid_mask = (newdf['weight'] >= 2) & (newdf['weight'] < 40)
        heavy_mask = (newdf['weight'] >= 40) & (newdf['weight'] < 140)
        vheavy_mask = (140 <= newdf['weight']) 
        newdf['weight_class'] = 'Light'
        newdf.loc[mid_mask, 'weight_class'] = 'Mid_Sized'
        newdf.loc[heavy_mask, 'weight_class'] = 'Heavy'
        newdf.loc[vheavy_mask, 'weight_class'] = 'Truck_Required'
        newdf.weight_class = newdf.weight_class.astype('string')

        return newdf
    
    def clean_orders_data(self, df): 
        newdf = df.copy()
        newdf = newdf.drop('level_0', axis=1)
        newdf = newdf.drop('first_name', axis=1)
        newdf = newdf.drop('last_name', axis=1)
        newdf = newdf.drop('1', axis=1)
        newdf['date_uuid'] = newdf['date_uuid'].astype('string')
        newdf['user_uuid'] = newdf['user_uuid'].astype('string')
        newdf['product_quantity'] = newdf['product_quantity'].astype('int')
        newdf['card_number'] = newdf['card_number'].astype('string')
        newdf['store_code'] = newdf['store_code'].astype('string')
        newdf['product_code'] = newdf['product_code'].astype('string')
        return newdf 
    
    def clean_date_data(self, df): 
        newdf = df.copy()
        newdf['timestamp'] = pd.to_datetime(newdf['timestamp'], format='%H:%M:%S', errors='coerce').dt.time
        newdf = newdf[newdf['timestamp'].notnull()]
        
        newdf['date_uuid'] = newdf['date_uuid'].astype('string')
        newdf['time_period'] = newdf['time_period'].astype('string')
        newdf['day'] = newdf['day'].astype('string')
        newdf['year'] = newdf['year'].astype('string')
        newdf['month'] = newdf['month'].astype('string')
        
        return newdf 
        
if __name__ == "__main__":
    datacleaner = DataCleaning() 
    df = pd.read_csv('products.csv')
    # df = datacleaner.convert_product_weights(df) 
    # datacleaner.clean_products_data(df)