/****** Script for extracting clv orders from RetinaAI StagingArea  ******/

--SAVE THIS FILE AS: RetinaAI_StagingArea_clv_orders.csv
--LOCATION: S Drive://Muzammil/Retina On Site/ Start Files - Extract from SQL

  SELECT REPLACE([order_id], ',', '/') as [order_id],
       REPLACE([customer_id], ',', '/') as [customer_id],
       REPLACE([order_timestamp], ',', '/') as [order_timestamp],
       REPLACE([order_revenue], ',', '/') as [order_revenue],
       REPLACE([order_gross_margin], ',', '/') as [order_gross_margin],
       REPLACE([order_discount_value], ',', '/') as [order_discount_value],
       REPLACE([is_gift_card_used], ',', '/') as [is_gift_card_used],
       REPLACE([total_tip_received], ',', '/') as [total_tip_received]
  FROM [RetinaAI_StagingArea].[clv].[orders]