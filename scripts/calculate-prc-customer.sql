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
