# Calculate click value per customer
CREATE OR REPLACE TABLE `prj.dataset.af_click_value`
AS 
  SELECT 
    m_clicks.CustomerId,
    COUNT(m_clicks.ClicksPerMonth) AS TotalMonths,
    AVG(IFNULL(m_orders.OrdersPerMonth, 1) / m_clicks.ClicksPerMonth) AS AvgClickValue
  FROM `prj.dataset.af_clicks_per_month` AS m_clicks
  INNER JOIN `prj.dataset.af_orders_per_month` AS m_orders
  ON 
    m_orders.CustomerId = m_clicks.CustomerId
    AND m_orders.Month = m_clicks.Month
    AND m_orders.Year = m_clicks.Year
  GROUP BY 1
  ORDER BY AvgClickValue DESC