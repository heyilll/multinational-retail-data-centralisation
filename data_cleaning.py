import pandas as pd 

class DataCleaning:
    """
    Contains methods to clean data from each of the data sources.
    """ 
    pd.set_option('display.max_columns', None)
    
    def clean_user_data(self, df: pd.DataFrame):
        '''
        Performs the cleaning of the user data. Cleans the user data, looks out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.

                Parameters:
                        df (pd.DataFrame): pandas Dataframe

                Returns:
                        ndf (pd.Dataframe): pandas Dataframe
        ''' 
        ndf = df.copy()
        
        ndf['date_of_birth'] = pd.to_datetime(ndf['date_of_birth'], errors='coerce')
        ndf['join_date'] = pd.to_datetime(ndf['join_date'], errors='coerce')
        ndf['country_code'].replace({'GGB': 'GB'}, inplace = True)
        ndf = ndf[ndf['country_code'].str.len() == 2]
        return ndf
    
    def clean_card_data(self, df: pd.DataFrame):
        '''
        Performs the cleaning of the card data. This method cleans the card data and removes any erroneous values, NULL values or errors with formatting.
                Parameters:
                        df (pd.DataFrame): pandas Dataframe

                Returns:
                        newdf (pd.Dataframe): pandas Dataframe
        ''' 
        def removeq(x):
            '''
            Removes any erroneous '?' from a string.
                    Parameters:
                            x (str): pandas Dataframe

                    Returns:
                            x (str): pandas Dataframe
            ''' 
            if '?' in x:
                return x.replace('?', '')
            else:
                return x
            
        newdf = df.copy()
        
        newdf['date_payment_confirmed'] = pd.to_datetime(newdf['date_payment_confirmed'], errors='coerce')
        newdf = newdf[newdf['expiry_date'].str.len() == 5]

        newdf.card_number = newdf.card_number.astype('string')
        newdf.card_number = newdf.card_number.apply(removeq)
        return newdf
    
    def clean_store_data(self, df):
        '''
        Performs the cleaning of the store data. 
        Cleans the data retrieve from the API and returns a pandas DataFrame.

                Parameters:
                        df (pd.DataFrame): pandas Dataframe

                Returns:
                        newdf (pd.Dataframe): pandas Dataframe
        ''' 
        newdf = df.copy()
        
        newdf['opening_date'] = pd.to_datetime(newdf['opening_date'], errors='coerce')
        newdf = newdf[newdf['country_code'].str.len() == 2]
        
        newdf.staff_numbers = newdf.staff_numbers.str.extract('(\d+)', expand=False)
        newdf.staff_numbers = newdf.staff_numbers.astype('int')
        
        newdf.latitude = newdf.latitude.astype('float')
        newdf.longitude = newdf.longitude.astype('float')
        newdf['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'}, inplace = True)
        
        newdf = newdf.drop('lat', axis=1)
        newdf = newdf.drop('level_0', axis=1) 
        return newdf
        
    def convert_product_weights(self, df):
        '''
        Performs the cleaning of the user data. Convert them all to a decimal value representing their weight in kg. 
        Uses a 1:1 ratio of ml to g as a rough estimate for the rows containing ml and 1:0.0283495 of oz to kg. It also cleans up the weight column 
        and removes all excess characters then represents the weights as a float.

                Parameters:
                        df (pd.DataFrame): pandas Dataframe

                Returns:
                        ndf (pd.Dataframe): pandas Dataframe
        ''' 
        def func_x(x): 
            '''
            Checks if the string contains ' x ' and multiplies the values and returns the result. Else, it checks for and removes any other 
            spaces or other erroneous values.
                    Parameters:
                            x (pd.DataFrame): pandas Dataframe

                    Returns:
                            x (pd.Dataframe): pandas Dataframe
            ''' 
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
            '''
            Checks whether the second to last index of a string is a digit and adds a character to the string if so. This is to aid the func_kg()
            function.
                    Parameters:
                            x (pd.DataFrame): pandas Dataframe

                    Returns:
                            x (pd.Dataframe): pandas Dataframe
            ''' 
            if x[-2].isdigit():
                return x + 'g'
            else: 
                return x
       
        def func_kg(x): 
            '''
            Takes in a string to convert to kilograms and returns a string representation of a float without 'kg'. The method removes the last two characters of 
            a string so that just digits remain and does the corresponding operation based on what units it is.
                    Parameters:
                            x (pd.DataFrame): pandas Dataframe

                    Returns:
                            x (pd.Dataframe): pandas Dataframe
            ''' 
            if x[-2:] == 'oz':
                return (float(x[:-2]) * 0.0283495)            
            elif x[-2:] != "kg":
                return float(x[:-2])/1000
            else: 
                return float(x[:-2])
            
        newdf = df.copy()
        newdf['date_added'] = pd.to_datetime(newdf['date_added'], errors='coerce')
        newdf = newdf[newdf["product_code"].str[2] == '-']
        newdf['weight'] = newdf['weight'].astype('string')
        newdf['weight'] = newdf['weight'].apply(func_x)
        newdf['weight'] = newdf['weight'].apply(func_g)
        newdf['weight'] = newdf['weight'].apply(func_kg)
        newdf['weight'] = newdf['weight'].astype('float')
        return newdf
    
    def clean_products_data(self, df): 
        '''
        Performs the cleaning of the products data. 

                Parameters:
                        df (pd.DataFrame): pandas Dataframe

                Returns:
                        ndf (pd.Dataframe): pandas Dataframe
        ''' 
        def removep(x):
            '''
            Removes any '£' from a string. 
            
                Parameters:
                        x (str): string

                Returns:
                        x (str): string
            ''' 
            if x[0] == '£':
                return x[1:]
            else:
                return x
            
        newdf = df.copy()
           
        newdf = newdf.rename(columns = {'removed': 'still_available'})   
        newdf.loc[newdf["still_available"] == "Still_avaliable", "still_available"] = True
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

        return newdf
    
    def clean_orders_data(self, df): 
        '''
        Performs the cleaning of the orders data. 

                Parameters:
                        df (pd.DataFrame): pandas Dataframe

                Returns:
                        ndf (pd.Dataframe): pandas Dataframe
        ''' 
        newdf = df.copy()
        newdf = newdf.drop('level_0', axis=1)
        newdf = newdf.drop('first_name', axis=1)
        newdf = newdf.drop('last_name', axis=1)
        newdf = newdf.drop('1', axis=1) 
        newdf['product_quantity'] = newdf['product_quantity'].astype('int') 
        return newdf 
    
    def clean_date_data(self, df):
        '''
        Performs the cleaning of the date data.  

                Parameters:
                        df (pd.DataFrame): pandas Dataframe

                Returns:
                        ndf (pd.Dataframe): pandas Dataframe
        ''' 
        newdf = df.copy()
        newdf['timestamp'] = pd.to_datetime(newdf['timestamp'], format='%H:%M:%S', errors='coerce').dt.time
        newdf = newdf[newdf['timestamp'].notnull()] 
        
        return newdf  