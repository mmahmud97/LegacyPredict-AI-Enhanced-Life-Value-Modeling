import pandas as pd

# Load the datasets
df1 = pd.read_csv("../2_Create customers_model_features/Result customers_model_features_1/1_customers_model_features_ready_for_early_model_imputation.csv")
df2 = pd.read_csv("../1_Create import_orders/Add_Transaction_Features_To_Customer_Model_Features/reformed_dataset.csv")

# Merge the datasets on 'customer_id'
merged_df = pd.merge(df1, df2, on='customer_id', how='left')

# Ensure that only the necessary columns are retained
# Assuming 'total_order_revenue' and 'total_counts_of_orders' are the columns to append
# If df1 already contains all necessary columns and you just want to add the new ones from df2, this step ensures no duplicate columns are created

# Output the merged dataset to a CSV file
merged_df.to_csv('Result customers_model_features_2/2_customer_model_features_read_for_early_model_imputation.csv', index=False)

# Print the first few rows to verify
print(merged_df.head())
print(merged_df.shape)