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