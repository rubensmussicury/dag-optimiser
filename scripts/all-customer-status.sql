# All customers [complete and incomplete] by status
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
  `prj.dataset.af_incomplete_customers`