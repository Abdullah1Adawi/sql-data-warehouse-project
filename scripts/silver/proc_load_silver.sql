/*
===============================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================
Script Puporse: 
	This stored procedure performs teh TL ( Extract, Transform, Load) process
	to populate teh 'silver' schema tables form the 'bronze' schema.
	It perfroms the following actions:
	- Truncate Silver tables.
	- Inserts transformed and cleaned data from Bronze into Silver tables.

Parameters:
	None.

Usage Example:
	EXEC silver.load_silver
===============================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	PRINT '====================================='
	PRINT 'Loading Silver Layer'
	PRINT '====================================='

	PRINT '-------------------------------------'
	PRINT 'Loading crm Tables'
	PRINT '-------------------------------------'

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_cust_info'
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data into: silver.crm_cust_info'
	INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_martial_status,
	cst_gndr,
	cst_create_date)

	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname, -- DATA Cleansing/ Handling unwanted spaces
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single' -- DATA Normalization and Standardization
		 WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
		 ELSE 'n/a' -- Data Cleansing/Handling Missing Data
	END cst_martial_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gender,
	cst_create_date
	FROM(
		SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	) AS t WHERE flag_last = 1; -- Data Cleansing/ Removing Duplicates
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------'


	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_prd_info'
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data into: silver.crm_prd_info'
	INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)

	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, --replace the minus for lower score to match the category table for joins later.
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,-- Replace the NULLS with zeros, it make calculating easier. Becareful this is allowed only if the business logic allows it
	CASE UPPER(TRIM(prd_line)) 
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt, -- Removing exact time as it has no purpose by casting both start and end date as DATE instead of DATETIME
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- Solving the problem where end date is before start date
	FROM bronze.crm_prd_info
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------'

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_sales_details'
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data into: silver.crm_sales_details'
	INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
	)

	SELECT 
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL -- Casting the integer date to varchar then to date (data type casting)
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)			   -- Removing any unvalid dates (handilng invalid data)
		END AS sls_order_dt,										   -- Doing these steps to all date colulmns
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price) --following the business rules that we assumed it works, we handeled missing and unvalid data
			THEN sls_quantity * ABS(sls_price)													  --using values derived from other columns
			ELSE sls_sales																		  --done this for sales and price columns using sales quantity and price columns
		END AS sls_sales,
		sls_quantity ,
		CASE WHEN sls_price IS NULL OR sls_price <=0
			THEN sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price
		END AS sls_price 
	FROM bronze.crm_sales_details
	WHERE sls_cust_id IN (SELECT cst_id FROM silver.crm_cust_info)
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------'


	PRINT '-------------------------------------'
	PRINT 'Loading erp Tables'
	PRINT '-------------------------------------'


	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_cust_az12'
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data into: silver.erp_cust_az12'
	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)

	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Transform the cid to match and join it to the cust_info table 
		ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ( 'M','MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ( 'F','FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12
	/*  Check if the transformation is working by finding any unmatching cid from this table to the cust_info table
	WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)
	*/
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------'

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_loc_a101'
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting Data into: silver.erp_loc_a101'
	INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
	)

	SELECT
	REPLACE(cid, '-','') cid, -- Remove the dash to join the table with 'cst_key' from crm_cust_info table
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany' -- Normalize and handle missing or blank country codes
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
	FROM bronze.erp_loc_a101;
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------'

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2'
	INSERT INTO silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance
	)

	SELECT -- no need for any transformation, data is clean
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------'

	SET @batch_end_time = GETDATE();
		PRINT '====================================='
		PRINT 'Loading Silver Layer is Completed'
		PRINT '	- Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '====================================='

	END TRY
	BEGIN CATCH
		PRINT '=============================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=============================='
	END CATCH
END