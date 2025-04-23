/* 
Script Purpose:
  This script creates a database named 'DataWarehouse' after checking if it already exists.
  If the database exists , it is dropped and recreated. Additionally , the script sets up three schemas 
  within the database: 'bronze', 'silver', 'gold'.
*/



USE master;

-- if  database already exist then it first drop and recreate the database

IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK_IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

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
