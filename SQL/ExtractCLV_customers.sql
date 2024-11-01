/****** Script for extracting clv customers from RetinaAI StagingArea ******/

--SAVE THIS FILE AS: RetinaAI_StagingArea_clv_customers.csv
--LOCATION: S Drive://Muzammil/Retina On Site/ Start Files - Extract from SQL

SELECT REPLACE([customer_id], ',', '/') as [customer_id],
       REPLACE([email], ',', '/') as [email],
       REPLACE([First_Name], ',', '/') as [First_Name],
       REPLACE([last_name], ',', '/') as [last_name],
       REPLACE([billaddr_1], ',', '/') as [billaddr_1],
       REPLACE([billaddr_3], ',', '/') as [billaddr_3],
       REPLACE([billaddr_city], ',', '/') as [billaddr_city],
       REPLACE([billaddr_country], ',', '/') as [billaddr_country],
       REPLACE([billaddr_zip], ',', '/') as [billaddr_zip],
       REPLACE([phone], ',', '/') as [phone],
       REPLACE([certification_status], ',', '/') as [certification_status],
       REPLACE([ssssoid], ',', '/') as [ssssoid],
       REPLACE([certification_start_date], ',', '/') as [certification_start_date],
       REPLACE([certification_end_date], ',', '/') as [certification_end_date],
       REPLACE([company_size], ',', '/') as [company_size],
       REPLACE([job_title], ',', '/') as [job_title],
       REPLACE([job_function], ',', '/') as [job_function],
       REPLACE([position_level_standard_job_title], ',', '/') as [position_level_standard_job_title],
       REPLACE([industry_category], ',', '/') as [industry_category],
       REPLACE([birthdate], ',', '/') as [birthdate],
       REPLACE([gender], ',', '/') as [gender],
       REPLACE([ethnicity], ',', '/') as [ethnicity],
       REPLACE([hr_department_size], ',', '/') as [hr_department_size],
       REPLACE([degree_type], ',', '/') as [degree_type],
       REPLACE([shrm_join_date], ',', '/') as [shrm_join_date],
       REPLACE([membership_paid_through_date], ',', '/') as [membership_paid_through_date],
       REPLACE([membership_type], ',', '/') as [membership_type],
       REPLACE([membership_subtype], ',', '/') as [membership_subtype],
       REPLACE([customer_type], ',', '/') as [customer_type],
       REPLACE([customer_segment], ',', '/') as [customer_segment],
       REPLACE([member_segment], ',', '/') as [member_segment],
       REPLACE([membership_status], ',', '/') as [membership_status]
  FROM [RetinaAI_StagingArea].[clv].[customers]
