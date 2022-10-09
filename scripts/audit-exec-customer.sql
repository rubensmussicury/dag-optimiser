# Fill audit execution for customer.
INSERT INTO `prj.dataset.audit_execution`
    (TableId, LastExecution, TotalIngested) 
VALUES
    ("af_all_customers_status", CURRENT_TIMESTAMP(), (SELECT COUNT(1) FROM `prj.dataset.af_all_customers_status`)),
    ("af_quality_customers", CURRENT_TIMESTAMP(), (SELECT COUNT(1) FROM `prj.dataset.af_quality_customers`)),
    ("af_complete_customers", CURRENT_TIMESTAMP(), (SELECT COUNT(1) FROM `prj.dataset.af_complete_customers`)),
    ("af_incomplete_customers", CURRENT_TIMESTAMP(), (SELECT COUNT(1) FROM `prj.dataset.af_incomplete_customers`)),
    ("af_pii_customer", CURRENT_TIMESTAMP(), (SELECT COUNT(1) FROM `prj.dataset.af_pii_customer`))