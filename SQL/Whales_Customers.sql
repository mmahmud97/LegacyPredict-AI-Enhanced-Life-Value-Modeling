SELECT 
    REPLACE([customer_id], ',', '/') as [customer_id],
    SUM(CAST(REPLACE([order_revenue], ',', '/') as float)) as total_revenue
FROM 
    [RetinaAI_StagingArea].[clv].[orders]
GROUP BY 
    [customer_id]
ORDER BY 
    total_revenue DESC
OFFSET 0 ROWS
FETCH NEXT 100 ROWS ONLY;
