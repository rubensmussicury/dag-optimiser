# Fill audit execution for click.
INSERT INTO `prj.dataset.audit_execution`
    (TableId, LastExecution, TotalIngested) 
VALUES
    ("af_click_value", CURRENT_TIMESTAMP(), (SELECT COUNT(1) FROM `prj.dataset.af_click_value`)),
    ("af_clicks_per_month", CURRENT_TIMESTAMP(), (SELECT COUNT(1) FROM `prj.dataset.af_clicks_per_month`)),
    ("af_clicks_above_10000", CURRENT_TIMESTAMP(), (SELECT COUNT(1) FROM `prj.dataset.af_clicks_above_10000`)),
    ("af_clicks_below_10000", CURRENT_TIMESTAMP(), (SELECT COUNT(1) FROM `prj.dataset.af_clicks_below_10000`))




