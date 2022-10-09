# Fill audit execution for order.
INSERT INTO `prj.dataset.audit_execution`
    (TableId, LastExecution, TotalIngested) 
VALUES
    ("af_orders_per_month", CURRENT_TIMESTAMP(), (SELECT COUNT(1) FROM `prj.dataset.af_orders_per_month`)),