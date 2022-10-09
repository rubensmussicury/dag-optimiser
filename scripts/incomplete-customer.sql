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