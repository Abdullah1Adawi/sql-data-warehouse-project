
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: No Result

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data Standardization and Consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info
-- --------------------------------------------
-- Check for unwanted Spaces
-- Expectiation: No Results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


-- Check for NULLS or Negative Numbers
-- Expectiation: No Results
SELECT *
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

-- Data Standardization and Consistency
SELECT DISTINCT(prd_line) AS prd_line
FROM silver.crm_prd_info

-- Check for Invalid Dates
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt
-- -----------------------------------------
-- Check for Invalid Dates
/*
SELECT
NULLIF(sls_due_dt,0) sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt <=0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <=0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

SELECT
NULLIF(sls_ship_dt,0) sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_ship_dt <=0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101
OR sls_ship_dt < 19000101
*/
-- Check for Invalid Date Orders
SELECT 
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt
OR sls_ship_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative
SELECT 
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=0
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price

FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL
OR sls_quantity IS NULL
OR sls_price IS NULL
OR sls_sales <=0
OR sls_quantity <=0
OR sls_price <=0

SELECT * 
FROM silver.crm_cust_info
WHERE cst_key NOT IN (SELECT cid FROM bronze.erp_cust_az12)

--------------------------------------------

-- Identify Out-of-Range Dates
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1900-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ( 'M','MALE') THEN 'Male'
	WHEN UPPER(TRIM(gen)) IN ( 'F','FEMALE') THEN 'Female'
	ELSE 'n/a'
END AS gen
FROM silver.erp_cust_az12

--------------------------

-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101