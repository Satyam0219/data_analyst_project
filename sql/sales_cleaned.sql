SELECT * FROM sales_raw;

-- ============================================
-- DATA PROFILING: sales_raw
-- ============================================

-- 1. Total Rows
SELECT 'total_rows' AS test, COUNT(*) AS result FROM sales_raw;

-- 2. Unique Order Count
SELECT 'unique_orders' AS test, COUNT(DISTINCT sls_ord_num) AS result FROM sales_raw;

-- 3. Duplicate Orders (Can highlight systemic duplication)
SELECT 'duplicate_orders' AS test, COUNT(*) AS result
FROM (
    SELECT sls_ord_num
    FROM sales_raw
    GROUP BY sls_ord_num
    HAVING COUNT(*) > 1
) t;

-- 4. Null / Empty String Checks
SELECT 'null_order_id' AS test, SUM(sls_ord_num IS NULL OR TRIM(sls_ord_num) = '') AS result FROM sales_raw
UNION ALL
SELECT 'null_product_key', SUM(sls_prd_key IS NULL OR TRIM(sls_prd_key) = '') FROM sales_raw
UNION ALL
SELECT 'null_customer_id', SUM(sls_cust_id IS NULL OR TRIM(sls_cust_id) = '') FROM sales_raw
UNION ALL
SELECT 'null_order_date', SUM(sls_order_dt IS NULL OR TRIM(sls_order_dt) = '') FROM sales_raw
UNION ALL
SELECT 'null_ship_date', SUM(sls_ship_dt IS NULL OR TRIM(sls_ship_dt) = '') FROM sales_raw
UNION ALL
SELECT 'null_due_date', SUM(sls_due_dt IS NULL OR TRIM(sls_due_dt) = '') FROM sales_raw
UNION ALL
SELECT 'null_sales', SUM(sls_sales IS NULL OR TRIM(sls_sales) = '') FROM sales_raw
UNION ALL
SELECT 'null_quantity', SUM(sls_quantity IS NULL OR TRIM(sls_quantity) = '') FROM sales_raw
UNION ALL
SELECT 'null_price', SUM(sls_price IS NULL OR TRIM(sls_price) = '') FROM sales_raw;

-- 5. Identify Invalid Numeric Values (Regex check)
SELECT 'invalid_sales' AS test, COUNT(*) AS result FROM sales_raw
WHERE sls_sales NOT REGEXP '^[0-9]+(\\.[0-9]+)?$'
UNION ALL
SELECT 'invalid_quantity', COUNT(*) FROM sales_raw
WHERE sls_quantity NOT REGEXP '^[0-9]+$'
UNION ALL
SELECT 'invalid_price', COUNT(*) FROM sales_raw
WHERE sls_price NOT REGEXP '^[0-9]+(\\.[0-9]+)?$';

-- 6. Invalid Date Formats (Expecting YYYY-MM-DD)
SELECT 'invalid_order_dt' AS test, COUNT(*) AS result FROM sales_raw
WHERE sls_order_dt NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
UNION ALL
SELECT 'invalid_ship_dt', COUNT(*) FROM sales_raw
WHERE sls_ship_dt NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
UNION ALL
SELECT 'invalid_due_dt', COUNT(*) FROM sales_raw
WHERE sls_due_dt NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- 7. Logical Date Checks (Dates shouldn't happen before the order is placed)
SELECT 'ship_before_order' AS test, COUNT(*) AS result FROM sales_raw
WHERE sls_ship_dt < sls_order_dt
UNION ALL
SELECT 'due_before_order', COUNT(*) FROM sales_raw
WHERE sls_due_dt < sls_order_dt;

-- 8. Negative or Zero Values Check
SELECT 'negative_sales' AS test, COUNT(*) AS result FROM sales_raw WHERE sls_sales <= 0
UNION ALL
SELECT 'zero_quantity', COUNT(*) FROM sales_raw WHERE sls_quantity <= 0
UNION ALL
SELECT 'negative_price', COUNT(*) FROM sales_raw WHERE sls_price <= 0;

-- 9. Sales Consistency Check (Total Sales should equal Price * Quantity)
SELECT 'sales_mismatch' AS test, COUNT(*) AS result FROM sales_raw
WHERE ABS((sls_quantity * sls_price) - sls_sales) > 1;

-- 10. Identify Whitespace Issues in Keys
SELECT 'whitespace_product_key' AS test, COUNT(*) AS result FROM sales_raw
WHERE sls_prd_key LIKE ' %' OR sls_prd_key LIKE '% '
UNION ALL
SELECT 'whitespace_customer_id', COUNT(*) FROM sales_raw
WHERE sls_cust_id LIKE ' %' OR sls_cust_id LIKE '% ';

-- 11. Statistical Outliers
SELECT 'min_sales' AS test, MIN(sls_sales) AS result FROM sales_raw
UNION ALL
SELECT 'max_sales', MAX(sls_sales) FROM sales_raw;

-- 12. Order Distribution Over Time
SELECT DATE(sls_order_dt) AS order_date, COUNT(*) AS total_orders
FROM sales_raw
GROUP BY DATE(sls_order_dt)
ORDER BY order_date;

-- 13. Duplicate Order + Product Combo (Detects if the same product was scanned twice on one order)
SELECT 'duplicate_order_product' AS test, COUNT(*) AS result FROM (
    SELECT sls_ord_num, sls_prd_key
    FROM sales_raw
    GROUP BY sls_ord_num, sls_prd_key
    HAVING COUNT(*) > 1
) t;

-- ============================================
-- CREATE CLEANED TABLE
-- ============================================

CREATE TABLE sales_cleaned AS
SELECT 
    sls_ord_num AS order_number,
    REPLACE(sls_prd_key,'-','_') AS product_key,
    sls_cust_id AS cust_id,
    
    -- Date transformations: Filters out bad lengths.
    -- (Note: Assumes length=8 implies an unformatted string like YYYYMMDD)
    CASE 
        WHEN sls_order_dt < 0 OR LENGTH(sls_order_dt) != 8 THEN NULL 
        ELSE CAST(sls_order_dt AS DATE) 
    END AS order_date,
    
    CASE 
        WHEN sls_ship_dt < 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL 
        ELSE CAST(sls_ship_dt AS DATE) 
    END AS ship_date,
    
    CASE 
        WHEN sls_due_dt < 0 OR LENGTH(sls_due_dt) != 8 THEN NULL 
        ELSE CAST(sls_due_dt AS DATE) 
    END AS due_date,
    
    -- Rectify Sales Logic: 
    -- If Sales is NULL, zero, or does not equal (Quantity * absolute Price), 
    -- forcefully recalculate it. Otherwise, keep original sales figure.
    CASE 
        WHEN sls_sales != (sls_quantity * ABS(sls_price)) 
             OR sls_sales IS NULL 
             OR sls_sales <= 0 
        THEN (sls_quantity * ABS(sls_price))
        ELSE sls_sales
    END AS sales,
    
    sls_quantity AS quantity,
    
    -- Rectify Price Logic:
    -- If price is missing or zero, backfill it by dividing Sales by Quantity.
    -- Casts the final derived price to a signed number.
    CAST(
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0) -- NULLIF prevents Division by Zero errors
            ELSE sls_price 
        END AS SIGNED
    ) AS price 

FROM sales_raw;

SELECT * FROM sales_cleaned;