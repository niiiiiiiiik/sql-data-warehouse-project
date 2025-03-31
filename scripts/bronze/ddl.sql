CREATE OR REPLACE PROCEDURE bronze.create_bronze_tables()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Drop and create bronze.crm_cust_info
    RAISE NOTICE 'Dropping and recreating table bronze.crm_cust_info';
    EXECUTE 'DROP TABLE IF EXISTS bronze.crm_cust_info';
    EXECUTE '
        CREATE TABLE bronze.crm_cust_info (
            cst_id              INT,
            cst_key             VARCHAR(50),
            cst_firstname       VARCHAR(50),
            cst_lastname        VARCHAR(50),
            cst_marital_status  VARCHAR(50),
            cst_gndr            VARCHAR(50),
            cst_create_date     DATE
        )
    ';

    -- Drop and create bronze.crm_prd_info
    RAISE NOTICE 'Dropping and recreating table bronze.crm_prd_info';
    EXECUTE 'DROP TABLE IF EXISTS bronze.crm_prd_info';
    EXECUTE '
        CREATE TABLE bronze.crm_prd_info (
            prd_id       INT,
            prd_key      VARCHAR(50),
            prd_nm       VARCHAR(50),
            prd_cost     INT,
            prd_line     VARCHAR(50),
            prd_start_dt TIMESTAMP,
            prd_end_dt   TIMESTAMP
        )
    ';

    -- Drop and create bronze.crm_sales_details
    RAISE NOTICE 'Dropping and recreating table bronze.crm_sales_details';
    EXECUTE 'DROP TABLE IF EXISTS bronze.crm_sales_details';
    EXECUTE '
        CREATE TABLE bronze.crm_sales_details (
            sls_ord_num  VARCHAR(50),
            sls_prd_key  VARCHAR(50),
            sls_cust_id  INT,
            sls_order_dt INT,
            sls_ship_dt  INT,
            sls_due_dt   INT,
            sls_sales    INT,
            sls_quantity INT,
            sls_price    INT
        )
    ';

    -- Drop and create bronze.erp_loc_a101
    RAISE NOTICE 'Dropping and recreating table bronze.erp_loc_a101';
    EXECUTE 'DROP TABLE IF EXISTS bronze.erp_loc_a101';
    EXECUTE '
        CREATE TABLE bronze.erp_loc_a101 (
            cid    VARCHAR(50),
            cntry  VARCHAR(50)
        )
    ';

    -- Drop and create bronze.erp_cust_az12
    RAISE NOTICE 'Dropping and recreating table bronze.erp_cust_az12';
    EXECUTE 'DROP TABLE IF EXISTS bronze.erp_cust_az12';
    EXECUTE '
        CREATE TABLE bronze.erp_cust_az12 (
            cid    VARCHAR(50),
            bdate  DATE,
            gen    VARCHAR(50)
        )
    ';

    -- Drop and create bronze.erp_px_cat_g1v2
    RAISE NOTICE 'Dropping and recreating table bronze.erp_px_cat_g1v2';
    EXECUTE 'DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2';
    EXECUTE '
        CREATE TABLE bronze.erp_px_cat_g1v2 (
            id           VARCHAR(50),
            cat          VARCHAR(50),
            subcat       VARCHAR(50),
            maintenance  VARCHAR(50)
        )
    ';

    RAISE NOTICE '✅ Bronze tables created successfully!';
END;
$$;


CALL bronze.create_bronze_tables();
