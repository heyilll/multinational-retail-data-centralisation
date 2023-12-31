-- Q1
-- SELECT store.country_code AS country, COUNT(store.country_code) AS total_no_stores  
-- FROM dim_store_details as store
-- GROUP BY
-- 	store.country_code
-- ORDER BY
-- 	total_no_stores DESC;

-- Q2
-- SELECT store.locality AS locality, COUNT(store.country_code) AS total_no_stores  
-- FROM dim_store_details as store
-- GROUP BY
-- 	store.locality
-- ORDER BY
-- 	total_no_stores DESC LIMIT 7

-- Q3
-- SELECT sum(dim_products.product_price * orders_table.product_quantity) AS total_sales, dim_date_times.month AS month
-- FROM orders_table
--  JOIN dim_products
-- ON orders_table.product_code = dim_products.product_code
--  JOIN dim_date_times
-- ON orders_table.date_uuid = dim_date_times.date_uuid
-- GROUP BY
-- dim_date_times.month
-- ORDER BY
-- total_sales DESC LIMIT 6

-- Q4
-- SELECT count(orders_table.product_code) AS numbers_of_sales, sum(orders_table.product_quantity) AS product_quantity_count,
-- CASE dim_store_details.store_type WHEN 'Web Portal' THEN 'Web'
--                          ELSE 'Offline'
-- END	AS location 					 
-- FROM orders_table 
-- JOIN dim_store_details
-- ON orders_table.store_code = dim_store_details.store_code
-- GROUP BY  
-- CASE dim_store_details.store_type WHEN 'Web Portal' THEN 'Web'
--                          ELSE 'Offline'
-- END		
-- ORDER BY
-- product_quantity_count

-- Q5
-- SELECT dim_store_details.store_type, ROUND(sum(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales, ROUND(((sum(dim_products.product_price * orders_table.product_quantity))*100)::numeric/(SELECT SUM(dim_products.product_price * orders_table.product_quantity) FROM orders_table JOIN dim_products
-- ON orders_table.product_code = dim_products.product_code)::numeric, 2) AS percentage_total					 
-- FROM orders_table
-- JOIN dim_store_details
-- ON orders_table.store_code = dim_store_details.store_code
-- JOIN dim_products
-- ON orders_table.product_code = dim_products.product_code
-- GROUP BY  
-- dim_store_details.store_type
-- ORDER BY
-- total_sales DESC

-- Q6
-- SELECT ROUND(sum(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales, dim_date_times.year AS year, dim_date_times.month AS month 				 
-- FROM orders_table
-- JOIN dim_date_times
-- ON orders_table.date_uuid = dim_date_times.date_uuid
-- JOIN dim_products
-- ON orders_table.product_code = dim_products.product_code
-- GROUP BY  
-- year, month
-- ORDER BY
-- total_sales DESC LIMIT 10

-- Q7
-- SELECT sum(dim_store_details.staff_numbers) AS total_staff_numbers, dim_store_details.country_code AS country_code				 
-- FROM dim_store_details
-- GROUP BY  
-- country_code
-- ORDER BY
-- total_staff_numbers DESC

-- Q8
-- SELECT ROUND(sum(dim_products.product_price * orders_table.product_quantity)::numeric, 2) AS total_sales, dim_store_details.store_type AS store_type, dim_store_details.country_code AS country_code				 
-- FROM orders_table
-- JOIN dim_store_details
-- ON orders_table.store_code = dim_store_details.store_code
-- JOIN dim_products
-- ON orders_table.product_code = dim_products.product_code
-- WHERE country_code = 'DE' 
-- GROUP BY  
-- store_type, country_code
-- ORDER BY
-- total_sales 

-- Q9
-- with data as (
--   select dim_date_times.year AS year,  
--          dim_date_times.timestamp as timestamp,  
--          LEAD(dim_date_times.timestamp,1) 
-- over (partition by dim_date_times.day,dim_date_times.month, dim_date_times.year
--                      order by dim_date_times.timestamp) as NextT
--     from orders_table 
-- 	join dim_date_times 
-- 	on orders_table.date_uuid = dim_date_times.date_uuid)
	
-- SELECT year, Avg(NextT - timestamp)  AS actual_time_taken			 
-- FROM data 
-- GROUP BY  
-- year 
-- ORDER BY
-- actual_time_taken DESC LIMIT 5