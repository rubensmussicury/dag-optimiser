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