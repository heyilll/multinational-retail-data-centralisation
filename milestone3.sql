-- SELECT card_number
-- FROM dim_card_details
-- WHERE char_length(card_number) = (SELECT max(char_length(card_number)) FROM dim_card_details )

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid, 
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT;

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255), 
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN join_date TYPE DATE;

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE float, 
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE float,
ALTER COLUMN continent TYPE VARCHAR(255),
ALTER COLUMN staff_numbers TYPE SMALLINT;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE float,
ALTER COLUMN uuid TYPE uuid USING uuid::uuid, 
ALTER COLUMN weight TYPE float,
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN still_available TYPE BOOL,
ALTER COLUMN weight_class TYPE VARCHAR(14);

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE;