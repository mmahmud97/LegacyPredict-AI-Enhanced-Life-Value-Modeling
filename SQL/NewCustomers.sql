SELECT DISTINCT 
    REPLACE([customer_id], ',', '/') as [customer_id]
FROM 
    [RetinaAI_StagingArea].[clv].[orders] o
WHERE 
    [order_timestamp] = (
        SELECT 
            MIN([order_timestamp])
        FROM 
            [RetinaAI_StagingArea].[clv].[orders] 
        WHERE 
            [customer_id] = o.[customer_id]
    )
    AND [order_timestamp] >= DATEADD(MONTH, -3, GETDATE());
