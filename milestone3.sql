-- ALTER TABLE orders_table
-- ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid, 
-- ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
-- ALTER COLUMN card_number TYPE VARCHAR(19),
-- ALTER COLUMN store_code TYPE VARCHAR(12),
-- ALTER COLUMN product_code TYPE VARCHAR(11),
-- ALTER COLUMN product_quantity TYPE SMALLINT;

-- ALTER TABLE dim_users
-- ALTER COLUMN first_name TYPE VARCHAR(255), 
-- ALTER COLUMN last_name TYPE VARCHAR(255),
-- ALTER COLUMN country_code TYPE VARCHAR(2),
-- ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
-- ALTER COLUMN date_of_birth TYPE DATE,
-- ALTER COLUMN join_date TYPE DATE;

-- ALTER TABLE dim_store_details
-- ALTER COLUMN longitude TYPE FLOAT, 
-- ALTER COLUMN opening_date TYPE DATE,
-- ALTER COLUMN store_code TYPE VARCHAR(12),
-- ALTER COLUMN locality TYPE VARCHAR(255),
-- ALTER COLUMN country_code TYPE VARCHAR(2),
-- ALTER COLUMN store_type TYPE VARCHAR(255),
-- ALTER COLUMN latitude TYPE FLOAT,
-- ALTER COLUMN continent TYPE VARCHAR(255),
-- ALTER COLUMN staff_numbers TYPE SMALLINT;

-- ALTER TABLE dim_products
-- ALTER COLUMN product_price TYPE float,
-- ALTER COLUMN uuid TYPE uuid USING uuid::uuid, 
-- ALTER COLUMN weight TYPE float,
-- ALTER COLUMN "EAN" TYPE VARCHAR(17),
-- ALTER COLUMN product_code TYPE VARCHAR(11),
-- ALTER COLUMN date_added TYPE DATE,
-- ALTER COLUMN still_available TYPE BOOL,
-- ALTER COLUMN weight_class TYPE VARCHAR(14);

-- ALTER TABLE dim_date_times
-- ALTER COLUMN month TYPE VARCHAR(2),
-- ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
-- ALTER COLUMN year TYPE VARCHAR(4),
-- ALTER COLUMN day TYPE VARCHAR(2),
-- ALTER COLUMN time_period TYPE VARCHAR(10);

-- ALTER TABLE dim_card_details
-- ALTER COLUMN card_number TYPE VARCHAR(19),
-- ALTER COLUMN expiry_date TYPE VARCHAR(5),
-- ALTER COLUMN date_payment_confirmed TYPE DATE;

-- ALTER TABLE dim_users
-- ADD PRIMARY KEY (user_uuid);  

-- ALTER TABLE dim_store_details
-- ADD PRIMARY KEY (store_code);

-- ALTER TABLE dim_products
-- ADD PRIMARY KEY (product_code);

-- ALTER TABLE dim_date_times
-- ADD PRIMARY KEY (date_uuid);

-- ALTER TABLE dim_card_details
-- ADD PRIMARY KEY (card_number);

-- ALTER TABLE orders_table
-- ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid),
-- ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
-- ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
-- ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
-- ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number); 