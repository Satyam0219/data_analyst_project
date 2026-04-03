/* =========================================================
   SALES & CUSTOMER DATA ANALYSIS REPORT
   Format: MySQL
   ========================================================= */

-- ---------------------------------------------------------
--   EXECUTIVE KPIs 
-- ---------------------------------------------------------

/* WHAT: Basic financial and volume totals.
   WHY:  Essential for high-level tracking of total revenue and reach. */

SELECT 
    SUM(price) AS total_sales 
FROM sales_cleaned;

SELECT 
    SUM(quantity) AS total_quantity 
FROM sales_cleaned;

SELECT 
 round(sum(sales)/count(distinct order_number)) AS avg_order_value
FROM sales_cleaned;

SELECT 
    COUNT(DISTINCT order_number) AS total_orders 
FROM sales_cleaned;

SELECT 
    COUNT(DISTINCT product_name) AS total_products 
FROM cleaned_products;

SELECT 
    COUNT(customer_key) AS total_customers 
FROM cleaned_customer;

/* WHAT: Unified KPI report.
   WHY:  Consolidates all core metrics into a single table for easy dashboard integration. */

SELECT 
    'Total Sales' AS measure_name, 
    SUM(sales) AS measure_value 
FROM sales_cleaned
UNION ALL
SELECT 
    'Total Quantity', 
    SUM(quantity) 
FROM sales_cleaned
UNION ALL
SELECT 
    'Average Price', 
    AVG(price) 
FROM sales_cleaned
UNION ALL
SELECT 
    'Total Orders', 
    COUNT(DISTINCT order_number) 
FROM sales_cleaned
UNION ALL
SELECT 
    'Total Products', 
    COUNT(DISTINCT product_name) 
FROM cleaned_products
UNION ALL
SELECT 
    'Total Customers', 
    COUNT(customer_id) 
FROM cleaned_customer;


-- ---------------------------------------------------------
--   DEMOGRAPHICS & CATEGORICAL BREAKDOWN
-- ---------------------------------------------------------

/* WHAT: Distribution of customers and revenue by segment.
   WHY:  Identifies geographic strongholds and top-performing product categories. */

SELECT
    country,
    COUNT(customer_key) AS total_customers
FROM cleaned_customer
GROUP BY 
    country
ORDER BY 
    total_customers DESC;

SELECT
    gender,
    COUNT(customer_key) AS total_customers
FROM cleaned_customer
GROUP BY 
    gender
ORDER BY 
    total_customers DESC;

SELECT
    cat,
    COUNT(product_key) AS total_products
FROM cleaned_products
GROUP BY 
    cat
ORDER BY 
    total_products DESC;

SELECT
    cat,
    AVG(product_cost) AS avg_cost
FROM cleaned_products
GROUP BY 
    cat
ORDER BY 
    avg_cost DESC;

SELECT
    p.cat,
    SUM(f.sales) AS total_revenue
FROM sales_cleaned f
LEFT JOIN cleaned_products p
    ON f.product_key = p.product_key
GROUP BY 
    p.cat
ORDER BY 
    total_revenue DESC;


-- ---------------------------------------------------------
--   PERFORMANCE RANKINGS (TOP & BOTTOM)
-- ---------------------------------------------------------

/* WHAT: Ranking products and customers by performance.
   WHY:  Prioritizes high-value products/customers and flags at-risk accounts. */

-- Top 5 Products by Revenue (Complex Ranking)
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(f.sales) AS total_revenue,
        RANK() OVER (ORDER BY SUM(f.sales) DESC) AS rank_products
    FROM sales_cleaned f
    LEFT JOIN cleaned_products p
        ON p.product_key = f.product_key
    GROUP BY 
        p.product_name
) AS ranked_products
WHERE 
    rank_products <= 5;

-- 5 Worst-Performing Products
SELECT 
    p.product_name,
    SUM(f.sales) AS total_revenue
FROM sales_cleaned f
LEFT JOIN cleaned_products p
    ON p.product_key = f.product_key
GROUP BY 
    p.product_name
ORDER BY 
    total_revenue ASC
LIMIT 5;

-- Top 10 Customers by Revenue
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(f.sales) AS total_revenue
FROM sales_cleaned f
LEFT JOIN cleaned_customer c
    ON c.customer_id = f.cust_id
GROUP BY 
    c.customer_id,
    c.first_name,
    c.last_name
ORDER BY 
    total_revenue DESC
LIMIT 10;

-- 3 Customers with Fewest Orders
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT order_number) AS total_orders
FROM sales_cleaned f
LEFT JOIN cleaned_customer c
    ON c.customer_id = f.cust_id
GROUP BY 
    c.customer_id,
    c.first_name,
    c.last_name
ORDER BY 
    total_orders ASC
LIMIT 3;


-- ---------------------------------------------------------
--   TRENDS & YEAR-OVER-YEAR (YoY) GROWTH
-- ---------------------------------------------------------

/* WHAT: Analysis of performance over time.
   WHY:  Tracks seasonality and growth health relative to historical data. */

-- Monthly Sales breakdown
SELECT
    DATE_FORMAT(order_date, '%Y-%m-01') AS order_month,
    SUM(sales) AS total_sales,
    COUNT(DISTINCT cust_id) AS total_customers,
    SUM(quantity) AS total_quantity
FROM sales_cleaned
WHERE 
    order_date IS NOT NULL
GROUP BY 
    order_month
ORDER BY 
    order_month;

-- Running Total & Moving Average
SELECT
    order_date,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
    AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM (
    SELECT 
        DATE_FORMAT(order_date, '%Y-01-01') AS order_date,
        SUM(sales) AS total_sales,
        AVG(price) AS avg_price
    FROM sales_cleaned
    WHERE 
        order_date IS NOT NULL
    GROUP BY 
        order_date
) t;

-- Year-over-Year Product Analysis
WITH yearly_product_sales AS (
    SELECT
        YEAR(f.order_date) AS order_year,
        p.product_name,
        SUM(f.sales) AS current_sales
    FROM sales_cleaned f
    LEFT JOIN cleaned_products p
        ON f.product_key = p.product_key
    WHERE 
        f.order_date IS NOT NULL
    GROUP BY 
        order_year,
        p.product_name
)
SELECT
    order_year,
    product_name,
    current_sales,
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
    CASE 
        WHEN current_sales > LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) THEN 'Increase'
        WHEN current_sales < LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) THEN 'Decrease'
        ELSE 'No Change'
    END AS py_change
FROM yearly_product_sales
ORDER BY 
    product_name, 
    order_year;
