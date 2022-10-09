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