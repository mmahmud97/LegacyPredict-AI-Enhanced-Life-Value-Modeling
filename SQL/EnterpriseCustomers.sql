SELECT DISTINCT o.customer_id
FROM [RetinaAI_StagingArea].[clv].[orders] o
INNER JOIN [Analytics].[dbo].[ENT_AllTransactions] c ON o.customer_id = c.customer_id;