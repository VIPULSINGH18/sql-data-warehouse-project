EXEC bronze.load_bronze  /*executing the stored procedure*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS  /*this is stored procedure code which helps us to load data
												  anytime and in any query without writing code again and again*/ 
BEGIN		

	PRINT 'Loading CRM table';
BEGIN TRY 
	PRINT '==================================';
	PRINT 'Truncating table';
	PRINT '==================================' ;
	TRUNCATE TABLE bronze.crm_cust_info;

	PRINT '==================================';
	PRINT 'Inserting table';
	PRINT '==================================';
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_crm\cust_info.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);


	/*SELECT * FROM bronze.crm_cust_info        displaying the data 
	SELECT COUNT(*) FROM bronze.crm_cust_info     count of rows */




	PRINT '==================================';
	PRINT 'Truncating table';
	PRINT '==================================' ;
	TRUNCATE TABLE bronze.crm_prd_info;

	PRINT '==================================';
	PRINT 'Inserting table';
	PRINT '==================================';
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_crm\prd_info.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);


	/*SELECT * FROM bronze.crm_prd_info        displaying the data 
	SELECT COUNT(*) FROM bronze.crm_prd_info     count of rows */





	PRINT '==================================';
	PRINT 'Truncating table';
	PRINT '==================================';
	TRUNCATE TABLE bronze.crm_sales_details;

	PRINT '==================================';
	PRINT 'Inserting table';
	PRINT '==================================';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_crm\sales_details.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);



	/*SELECT * FROM bronze.crm_sales_details       displaying the data 
	SELECT COUNT(*) FROM bronze.crm_sales_details     count of rows */



	PRINT 'Loading ERP table';

	PRINT '==================================';
	PRINT 'Truncating table';
	PRINT '==================================';
	TRUNCATE TABLE bronze.erp_cust_az12;

	PRINT '==================================';
	PRINT 'Inserting table';
	PRINT '==================================';
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_erp\cust_az12.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);


	/*SELECT * FROM bronze.erp_cust_az12        displaying the data 
	SELECT COUNT(*) FROM bronze.erp_cust_az12     count of rows */





	PRINT '==================================';
	PRINT 'Truncating table';
	PRINT '==================================';
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT '==================================';
	PRINT 'Inserting table';
	PRINT '==================================';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_erp\loc_a101.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);


	/* SELECT * FROM bronze.erp_loc_a101        displaying the data 
	SELECT COUNT(*) FROM bronze.erp_loc_a101     count of rows */





	PRINT '==================================';
	PRINT 'Truncating table';
	PRINT '==================================';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	PRINT '==================================';
	PRINT 'Inserting table';
	PRINT '==================================';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_erp\px_cat_g1v2.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);
END TRY

BEGIN CATCH
		PRINT '==================================';
	    PRINT 'error occured';
	    PRINT '==================================';
END CATCH





/* we can use date time function */

DECLARE @start_time DATETIME , @end_time DATETIME;  /* initialise the date time variable*/

SET @start_time = GETDATE();  /*startime of any code */
SET @end_time = GETDATE();		/*end time of any code*/

/* in this way we can find the difference between the start time and end time to
   find the load duration of any code */
