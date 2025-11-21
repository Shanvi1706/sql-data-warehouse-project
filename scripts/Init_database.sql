/*
==================================================================================
Create Database and Schemas
==================================================================================
Script Purpose:
      This script creates a new database named 'DataWarehouse' after checking if it already exists.
      If the database exists, it is dropped and recreated. Additionally, the sript sets up three schemas
      within the database: 'bronze', 'silver', and 'gold'.

WARNING:
      Running this script will drop the 'DataWarehouse' database if it exists.
      All data in the database will be permanent deleted. Proceed with caution
      and ensure you hae proper backups running this script.
*/
      
USE master;
GO

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
     ALTER DATABASE DataWarehouse SET SINGLE USER WITH ROLLBACK IMMEDIATE;
     DROP DATABASE DataWarehouse;
END;
GO

--Create the 'DataWarehouse' Database 
CREATE DATABASE DataWarehouse;
GO  

USE DataWarehouse;
GO

--Create Schemas   
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
