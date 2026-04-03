-- Create raw products table to accept all data as TEXT strings initially
CREATE TABLE products_raw (
    product_id TEXT,
    product_key TEXT,
    product_name TEXT,
    product_cost TEXT,
    product_line TEXT,
    product_start_date TEXT,
    product_end_date TEXT
);

-- Load the raw data from CSV
LOAD DATA LOCAL INFILE '/Users/mac/Downloads/products.csv'
INTO TABLE products_raw
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- ============================================
-- DATA PROFILING: products_raw
-- ============================================
-- The following queries perform sanity checks and anomaly detection

-- 1. Row Count
SELECT 'total_rows' AS test, COUNT(*) AS result FROM products_raw;

-- 2. Unique Product IDs
SELECT 'unique_prd_id' AS test, COUNT(DISTINCT product_id) AS result FROM products_raw;

-- 3. Check for Duplicate Product IDs
SELECT 'duplicate_prd_id' AS test, COUNT(*) AS result
FROM (
    SELECT product_id
    FROM products_raw
    GROUP BY product_id
    HAVING COUNT(*) > 1
) t;

-- 4. Check for NULL or Empty values across critical columns
SELECT 'null_prd_id' AS test, SUM(product_id IS NULL OR TRIM(product_id) = '') AS result FROM products_raw
UNION ALL
SELECT 'null_prd_nm', SUM(product_name IS NULL OR TRIM(product_name) = '') FROM products_raw
UNION ALL
SELECT 'null_prd_cost', SUM(product_cost IS NULL OR TRIM(product_cost) = '') FROM products_raw
UNION ALL
SELECT 'null_start_dt', SUM(product_start_date IS NULL OR TRIM(product_start_date) = '') FROM products_raw
UNION ALL
SELECT 'null_end_dt', SUM(product_end_date IS NULL OR TRIM(product_end_date) = '') FROM products_raw;

-- 5. Validate Product Cost Format (must be an integer or decimal)
SELECT 'invalid_prd_cost' AS test, COUNT(*) AS result
FROM products_raw
WHERE product_cost NOT REGEXP '^[0-9]+(\\.[0-9]+)?$';

-- 6. Validate Date Formats (must be YYYY-MM-DD)
SELECT 'invalid_start_dt' AS test, COUNT(*) AS result
FROM products_raw
WHERE product_start_date NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
UNION ALL
SELECT 'invalid_end_dt', COUNT(*)
FROM products_raw
WHERE product_end_date NOT REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

-- 7. Logical Date Constraints (End Date cannot be before Start Date)
SELECT 'end_before_start' AS test, COUNT(*) AS result
FROM products_raw
WHERE product_end_date < product_start_date;

-- 8. Identify Whitespace Issues in Product Names
SELECT 'whitespace_prd_nm' AS test, COUNT(*) AS result
FROM products_raw
WHERE product_name LIKE ' %' OR product_name LIKE '% ';

-- 9. Check for Invalid Product ID Formats (non-numeric characters)
SELECT 'invalid_prd_id_format' AS test, COUNT(*) AS result
FROM products_raw
WHERE product_id NOT REGEXP '^[0-9]+$';

-- 10. Identify Cost Outliers (Min/Max boundaries)
SELECT 'min_cost' AS test, MIN(product_cost) AS result FROM products_raw
UNION ALL
SELECT 'max_cost', MAX(product_cost) FROM products_raw;

-- 11. Profile the distribution of product categories
SELECT product_line, COUNT(*) AS total_products
FROM products_raw
GROUP BY product_line
ORDER BY total_products DESC;

-- 12. Check for Duplicate Product Keys
SELECT 'duplicate_prd_key' AS test, COUNT(*) AS result
FROM (
    SELECT product_key
    FROM products_raw
    GROUP BY product_key
    HAVING COUNT(*) > 1
) t;

-- 13. Detect Hidden Characters (Tabs, Newlines) in Product Names
SELECT 'hidden_chars_prd_nm' AS test, COUNT(*) AS result
FROM products_raw
WHERE product_name LIKE '%\t%' OR product_name LIKE '%\n%';

-- ============================================
-- CREATE CLEANED TABLE
-- ============================================

CREATE TABLE products_cleaned AS 
SELECT 
    -- Cast ID to integer
    CAST(product_id AS SIGNED) AS product_id,
    
    -- Extract the first 5 characters as Category ID and replace hyphens with underscores
    REPLACE(SUBSTRING(product_key, 1, 5), '-', '_') AS cat_id,
    
    -- Extract the remainder of the key as the true Product Key and replace hyphens
    REPLACE(SUBSTRING(product_key, 7, LENGTH(product_key)), '-', '_') AS product_key,
    
    product_name,
    product_cost,
    
    -- Expand product line abbreviations into full descriptive names
    CASE 
        WHEN UPPER(TRIM(product_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(product_line)) = 'R' THEN 'Road' 
        WHEN UPPER(TRIM(product_line)) = 'S' THEN 'Other sales' 
        WHEN UPPER(TRIM(product_line)) = 'T' THEN 'Touring' 
        ELSE 'n/a'
    END AS product_line,
    
    -- Cast start date string to actual DATE format
    CAST(product_start_date AS DATE) AS product_start_date,
    
    -- Dynamically calculate the product_end_date using the LEAD function:
    -- We look at the NEXT product_start_date for the same product_key, subtract 1 day from it, 
    -- and assign that as the end date for the current record.
    CAST(
        DATE_FORMAT(
            DATE_SUB(
                LEAD(CAST(product_start_date AS DATE)) OVER (
                    PARTITION BY product_key 
                    ORDER BY CAST(product_start_date AS DATE)
                ),
                INTERVAL 1 DAY
            ),
            '%Y-%m-%d'
        ) AS DATE
    ) AS product_end_date

FROM products_raw;

SELECT * FROM products_cleaned;
