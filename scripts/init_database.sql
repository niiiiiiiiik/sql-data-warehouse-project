-- Drop the database if it exists
DROP DATABASE IF EXISTS "DataWarehouse";

-- Create a new database
CREATE DATABASE "DataWarehouse";

-- Create the schemas within DataWarehouse
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
