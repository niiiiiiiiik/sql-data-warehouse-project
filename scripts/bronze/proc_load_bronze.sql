/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
      - Truncates the bronze tables before loading data.
      - Uses the COPY command to load data from CSV files into bronze tables.
      
Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
    start_time      TIMESTAMP;
    end_time        TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
BEGIN
    batch_start_time := current_timestamp;
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    -- Loading CRM Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    start_time := current_timestamp;
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    EXECUTE 'TRUNCATE TABLE bronze.crm_cust_info';
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    EXECUTE $copy$
        COPY bronze.crm_cust_info
        FROM 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
        WITH (FORMAT csv, HEADER true)
    $copy$;
    end_time := current_timestamp;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    start_time := current_timestamp;
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    EXECUTE 'TRUNCATE TABLE bronze.crm_prd_info';
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    EXECUTE $copy$
        COPY bronze.crm_prd_info
        FROM 'C:\sql\dwh_project\datasets\source_crm\prd_info.csv'
        WITH (FORMAT csv, HEADER true)
    $copy$;
    end_time := current_timestamp;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    start_time := current_timestamp;
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    EXECUTE 'TRUNCATE TABLE bronze.crm_sales_details';
    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    EXECUTE $copy$
        COPY bronze.crm_sales_details
        FROM 'C:\sql\dwh_project\datasets\source_crm\sales_details.csv'
        WITH (FORMAT csv, HEADER true)
    $copy$;
    end_time := current_timestamp;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    -- Loading ERP Tables
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    start_time := current_timestamp;
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    EXECUTE 'TRUNCATE TABLE bronze.erp_loc_a101';
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    EXECUTE $copy$
        COPY bronze.erp_loc_a101
        FROM 'C:\sql\dwh_project\datasets\source_erp\loc_a101.csv'
        WITH (FORMAT csv, HEADER true)
    $copy$;
    end_time := current_timestamp;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    start_time := current_timestamp;
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    EXECUTE 'TRUNCATE TABLE bronze.erp_cust_az12';
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    EXECUTE $copy$
        COPY bronze.erp_cust_az12
        FROM 'C:\sql\dwh_project\datasets\source_erp\cust_az12.csv'
        WITH (FORMAT csv, HEADER true)
    $copy$;
    end_time := current_timestamp;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    start_time := current_timestamp;
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    EXECUTE 'TRUNCATE TABLE bronze.erp_px_cat_g1v2';
    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    EXECUTE $copy$
        COPY bronze.erp_px_cat_g1v2
        FROM 'C:\sql\dwh_project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (FORMAT csv, HEADER true)
    $copy$;
    end_time := current_timestamp;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

    batch_end_time := current_timestamp;
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE '==========================================';
END;
$$;
