'''In this file, we are using customer ids from import orders'''

import pandas as pd

# Read the CSV files
df1 = pd.read_csv("../1_Create import_orders/Result_Of_Create_Import_Orders/2_import_orders.csv")
df2 = pd.read_csv("../0_Start Files - Extract from SQL/RetinaAI_StagingArea_clv_customers.csv", low_memory=False)

# Display initial counts
print("Initial counts:")
print("Unique customer IDs in df1:", df1['customer_id'].nunique())
print("Unique customer IDs in df2:", df2['customer_id'].nunique())

# Extract customer IDs from df1
extracted_customer_ids = df1['customer_id'].unique()

# Filter df2 to keep only matching customer IDs
customer_data_set = df2[df2['customer_id'].isin(extracted_customer_ids)]

# Reset index to make sure it starts from 0
customer_data_set.reset_index(drop=True, inplace=True)

# Display final count
print("\nCounts after filtering:")
print("Unique customer IDs extracted from df1 (2_import_orders):", len(extracted_customer_ids))
print("Unique customer IDs in filtered df2 (customers):", customer_data_set['customer_id'].nunique())

customer_data_set.to_csv("../2_Create customers_model_features/Mid_Process_Transit_File_Repository/0_customers_model_features.csv")
print(len(customer_data_set))
