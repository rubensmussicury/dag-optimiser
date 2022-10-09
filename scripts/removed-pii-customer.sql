# Removing PII information from [customer]
CREATE OR REPLACE TABLE `prj.dataset.af_no_pii_customer`
AS 
  SELECT
    CustomerId,
    Gender,
    Nationality
  FROM
    `prj.dataset.af_complete_customers`;