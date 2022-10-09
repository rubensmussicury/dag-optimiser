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