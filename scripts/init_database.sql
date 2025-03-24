/*
=============================================================
Create Database and Schemas for DataWarehouse
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
=============================================================
*/

-- Connect to a database other than DataWarehouse (e.g., postgres) to execute these commands:

-- Drop the database if it exists
DROP DATABASE IF EXISTS "DataWarehouse";

-- Create a new database
CREATE DATABASE "DataWarehouse";


/*
=============================================================
Create Schemas in DataWarehouse
=============================================================
This script creates three schemas in the 'DataWarehouse' database: 
'bronze', 'silver', and 'gold'.
=============================================================
*/

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
