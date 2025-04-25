/*
USE master;

-- if  database already exist then first drop and recreate the database

IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK_IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO  */



/*
-- creating database 
CREATE DATABASE DataWarehouse;

-- using datawarehouse database
USE DataWarehouse;   

-- creating schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO




/* before creating the table we have to check whether the table exists or not */

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;

/* using DDL commands and creating tables of source crm and source erp */

CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);


IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_date DATETIME,
prd_end_date DATETIME
);


IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);



IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);


IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);


IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50), 
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);

*/


/* now we have created the table but it's empty so we have to load the data inside the table...
But before loading the data we have to write TRUNCATE function, assuming if already data is loaded
in past and if we will load the data again then there will be duplicacy of data*/




CREATE OR ALTER PROCEDURE bronze.load_bronze AS  /*this is stored procedure code which helps us to load data
												  anytime and in any query without writing code again and again*/ 

BEGIN
BEGIN TRY 

	TRUNCATE TABLE bronze.crm_cust_info;
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_crm\cust_info.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);


	/*SELECT * FROM bronze.crm_cust_info        displaying the data 
	SELECT COUNT(*) FROM bronze.crm_cust_info     count of rows */






	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_crm\prd_info.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);


	/*SELECT * FROM bronze.crm_prd_info        displaying the data 
	SELECT COUNT(*) FROM bronze.crm_prd_info     count of rows */






	TRUNCATE TABLE bronze.crm_sales_details;
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_crm\sales_details.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);


	/*SELECT * FROM bronze.crm_sales_details       displaying the data 
	SELECT COUNT(*) FROM bronze.crm_sales_details     count of rows */






	TRUNCATE TABLE bronze.erp_cust_az12;
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_erp\cust_az12.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);


	/*SELECT * FROM bronze.erp_cust_az12        displaying the data 
	SELECT COUNT(*) FROM bronze.erp_cust_az12     count of rows */






	TRUNCATE TABLE bronze.erp_loc_a101;
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_erp\loc_a101.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);


	/* SELECT * FROM bronze.erp_loc_a101        displaying the data 
	SELECT COUNT(*) FROM bronze.erp_loc_a101     count of rows */






	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\HP\OneDrive\Desktop\data warehouse project\source_erp\px_cat_g1v2.csv'
	WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
	);
END TRY

BEGIN CATCH
END CATCH

END


	/*SELECT * FROM bronze.erp_px_cat_g1v2        displaying the data 
	SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2     count of rows */


