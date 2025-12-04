/*
=====================================================================================
Create a database and schemas
=====================================================================================
Script purpose:
	This script creates a new database named 'DataWarehouse' after checking if it
	already exists. If the databse does exist, it is dropped and recreated.
	Additionally, the script sets up three schemas within the database:
	'bronze', 'silver' and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution
	and ensure you have proper backups before running this script.

*/

USE master;
GO

-- Drop and re-create the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'dataWarehouse')
-- the result here will be 1 if the 'Datawarehouse' exists in the system databases.
-- if not, the SELECT FUNCTION will not return anything (not even a 0 or null).
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO

-- create schemas
CREATE SCHEMA bronze;
GO -- this 'GO' is telling SQL to execute the command completely before proceeding to the next command. (it is necessary)
-- double check its there -> <database name> -> Security -> schemas
CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
