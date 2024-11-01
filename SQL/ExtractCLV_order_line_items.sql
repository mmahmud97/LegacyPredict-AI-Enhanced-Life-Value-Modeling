/****** Script for extracting clv order_line_items from RetinaAI StagingArea ******/

--SAVE THIS FILE AS: RetinaAI_StagingArea_clv_order_line_items.csv
--LOCATION: S Drive://Muzammil/Retina On Site/ Start Files - Extract from SQL

SELECT REPLACE([id], ',', '/') as [id],
       REPLACE([order_id], ',', '/') as [order_id],
       REPLACE([customer_id], ',', '/') as [customer_id],
       REPLACE([customer_email], ',', '/') as [customer_email],
       REPLACE([product_title], ',', '/') as [product_title],
       REPLACE([product_category], ',', '/') as [product_category],
       REPLACE([product_quantity], ',', '/') as [product_quantity],
       REPLACE([product_price], ',', '/') as [product_price],
       REPLACE([product_totalbaseprice], ',', '/') as [product_totalbaseprice],
       REPLACE([product_totalcharged], ',', '/') as [product_totalcharged],
       REPLACE([product_sku], ',', '/') as [product_sku],
       REPLACE([order_timestamp], ',', '/') as [order_timestamp]
  FROM [RetinaAI_StagingArea].[clv].[order_line_items]
