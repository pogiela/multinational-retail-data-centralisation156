
-- #### TASK 1: No. of stores in each country #### 

SELECT country_code, COUNT(country_code)
FROM dim_store_details
GROUP BY country_code


-- #### TASK 2: Which locations currently have the most stores #### 

SELECT locality, COUNT(locality) as count
FROM dim_store_details
GROUP BY locality
ORDER BY count DESC
LIMIT 7


-- #### TASK 3: Which months produced the largest amount of sales #### 

SELECT 
	ROUND(SUM(orders_table.product_quantity * CAST(dim_products.product_price as numeric)), 2) AS total_sales,
	dim_date_times.month
FROM orders_table
INNER JOIN dim_products
ON dim_products.product_code = orders_table.product_code
INNER JOIN dim_date_times
ON dim_date_times.date_uuid = orders_table.date_uuid
GROUP BY dim_date_times.month
ORDER BY total_sales DESC
LIMIT 6	
	

-- #### TASK 4: How many sales are coming from online #### 

SELECT
	COUNT(o.date_uuid) AS numbers_of_sales,
	SUM(o.product_quantity) AS product_quantity_count,
	CASE
		WHEN s.store_type IN ('Web Portal') THEN 'Web'
		ELSE 'Offline'
	END AS location
FROM orders_table as o
INNER JOIN dim_store_details as s
ON s.store_code = o.store_code
GROUP BY location
ORDER BY location DESC


-- #### TASK 5: What percentage of sales come through each type of store #### 

WITH total_sales_per_store_type AS (
	SELECT
		s.store_type,
		ROUND(SUM(o.product_quantity * CAST(p.product_price as numeric)), 2) AS total_sales
	FROM orders_table as o
	INNER JOIN dim_products as p
	ON p.product_code = o.product_code
	INNER JOIN dim_store_details as s
	ON s.store_code = o.store_code
	GROUP BY s.store_type
)
SELECT
	store_type,
	total_sales,
	ROUND( 100 * total_sales / SUM(total_sales) OVER (), 2) AS "percentage_total(%)"
FROM total_sales_per_store_type
GROUP BY store_type, total_sales
ORDER BY total_sales DESC


-- #### TASK 6: Which month in each year produced the highest cost of sales #### 

SELECT 
	ROUND(SUM(o.product_quantity * CAST(p.product_price as numeric)), 2) AS total_sales,
	t.year,
	t.month
FROM orders_table as o
INNER JOIN dim_products as p
ON p.product_code = o.product_code
INNER JOIN dim_date_times as t
ON t.date_uuid = o.date_uuid
GROUP BY t.year, t.month
ORDER BY total_sales DESC
LIMIT 10


-- #### TASK 7: What is our staff headcount #### 

SELECT
	SUM(staff_numbers) as total_staff_numbers,
	country_code
FROM
	dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC


-- #### TASK 8: Which German store type is selling the most #### 

SELECT 
	ROUND(SUM(o.product_quantity * CAST(p.product_price as numeric)), 2) AS total_sales,
	s.store_type,
	s.country_code
FROM orders_table AS o
INNER JOIN dim_products as p
ON p.product_code = o.product_code
INNER JOIN dim_store_details as s
ON s.store_code = o.store_code
WHERE s.country_code = 'DE'
GROUP BY s.country_code, s.store_type
ORDER BY total_sales


-- #### TASK 9: How quickly is the company making sales #### 

WITH sale_times AS (
	SELECT 
		year,
		(year || '-' || month || '-' || day || ' ' || timestamp)::timestamp AS sale_date_time,
		LEAD((year || '-' || month || '-' || day || ' ' || timestamp)::timestamp) 
			OVER (ORDER BY year, month, day, timestamp) AS next_sale_date_time,
		LEAD((year || '-' || month || '-' || day || ' ' || timestamp)::timestamp) 
			OVER (ORDER BY year, month, day, timestamp) - (year || '-' || month || '-' || day || ' ' || timestamp)::timestamp AS time_difference
	FROM dim_date_times
	ORDER BY year, month, day, timestamp
)
SELECT 
	year,
	(
		' "hours": ' || EXTRACT(HOUR FROM AVG(time_difference) ) || 
		' "minutes": ' || EXTRACT(MINUTE FROM AVG(time_difference) ) || 
		' "seconds": ' || EXTRACT(SECOND FROM AVG(time_difference) ) || 
		' "milliseconds": ' || EXTRACT(MILLISECOND FROM AVG(time_difference)) 
	) AS actual_time_taken
FROM sale_times
GROUP BY year
ORDER BY AVG(time_difference) DESC
LIMIT 5;

