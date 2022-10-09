
# Complete [customer]
CREATE OR REPLACE TABLE `prj.dataset.af_complete_customers`
AS 
  SELECT 
    *
  FROM 
    `prj.dataset.customer`
  WHERE
    SAFE_CAST(Birthdate AS DATE) IS NOT NULL
    AND Gender IS NOT NULL
    AND Nationality IS NOT NULL;

# Incomplete [customer]
CREATE OR REPLACE TABLE `prj.dataset.af_incomplete_customers`
AS 
  SELECT 
    *
  FROM 
    `prj.dataset.customer`
  WHERE
    SAFE_CAST(Birthdate AS DATE) IS NULL
    OR Gender IS NULL
    OR Nationality IS NULL;


# Removing PII information from [customer]
CREATE OR REPLACE TABLE `prj.dataset.af_pii_customer`
AS 
  SELECT
    CustomerId,
    Gender,
    Nationality
  FROM
    `prj.dataset.af_complete_customers`;


# Aggregate Clicks Per Month
CREATE OR REPLACE TABLE `prj.dataset.af_clicks_per_month`
AS 
  SELECT
    CustomerId,
    EXTRACT(YEAR FROM Day) AS Year,
    EXTRACT(MONTH FROM Day) AS Month,
    SUM(Clicks) AS ClicksPerMonth
  FROM
    `prj.dataset.clicks`
  GROUP BY
    CustomerId, Year, Month;


# Aggregate Orders Per Month
CREATE OR REPLACE TABLE `prj.dataset.af_orders_per_month`
AS 
  SELECT
    CustomerId,
    EXTRACT(YEAR FROM OrderDate) AS Year,
    EXTRACT(MONTH FROM OrderDate) AS Month,
    SUM(Amount) AS OrdersPerMonth
  FROM
    `prj.dataset.orders`
  GROUP BY
    CustomerId, Year, Month;


# Filter Clicks above 10000
CREATE OR REPLACE TABLE `prj.dataset.af_clicks_above_10000`
AS 
  SELECT
    CustomerId,
    Year,
    Month,
    ClicksPerMonth
  FROM
    `prj.dataset.af_clicks_per_month`
  WHERE
    ClicksPerMonth >= 10000;

# Filter Clicks below 10000 
CREATE OR REPLACE TABLE `prj.dataset.af_clicks_below_10000`
AS 
  SELECT
    CustomerId,
    Year,
    Month,
    ClicksPerMonth
  FROM
    `prj.dataset.af_clicks_per_month`
  WHERE
    ClicksPerMonth < 10000;

# Calculate % of complete and incomplete customers
CREATE OR REPLACE TABLE `prj.dataset.af_quality_customers`
AS 
  SELECT 
    COUNT(c_all.CustomerId) AS TotalCustomer, 
    COUNTIF(c_complete.CustomerId IS NOT NULL) / COUNT(c_all.CustomerId) AS Complete,
    COUNTIF(c_incomplete.CustomerId IS NOT NULL) / COUNT(c_all.CustomerId) AS Incomplete,
  FROM `prj.dataset.customer` AS c_all
  LEFT JOIN `prj.dataset.af_complete_customers` AS c_complete
  ON c_all.CustomerId = c_complete.CustomerId 
  LEFT JOIN `prj.dataset.af_incomplete_customers` AS c_incomplete
  ON c_all.CustomerId = c_incomplete.CustomerId;

# Calculate click value per customer
CREATE OR REPLACE TABLE `prj.dataset.af_click_value`
AS 
  SELECT 
    m_clicks.CustomerId,
    COUNT(m_clicks.ClicksPerMonth) AS TotalMonths,
    AVG(IFNULL(m_orders.OrdersPerMonth, 1) / m_clicks.ClicksPerMonth) AS AvgClickValue
  FROM `prj.dataset.af_clicks_per_month` AS m_clicks
  INNER JOIN `prj.dataset.af_orders_per_month` AS m_orders
  ON 
    m_orders.CustomerId = m_clicks.CustomerId
    AND m_orders.Month = m_clicks.Month
    AND m_orders.Year = m_clicks.Year
  GROUP BY 1
  ORDER BY AvgClickValue DESC;

CREATE OR REPLACE TABLE `prj.dataset.af_all_customers_status`
AS 
SELECT 
  *, 
  "complete" AS Status
FROM 
  `prj.dataset.af_complete_customers`

UNION ALL

SELECT 
  *, 
  "incomplete" AS Status
FROM
  `prj.dataset.af_incomplete_customers`;






