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