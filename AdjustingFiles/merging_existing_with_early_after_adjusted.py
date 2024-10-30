import pandas as pd
import os

df1 = pd.read_csv("../3_MachineLearningEarly/ResultFiles/matched_customers_20240125_122931.csv")
df2 = pd.read_csv("../1_Create import_orders/Result_Of_Create_Import_Orders/2_import_orders.csv")
print(len(df1))
print(len(df2))

transit_dataframe = df2[df2['customer_id'].isin(df1['similar_reference_customer_id'])]

# Step 2: Merge df1 with transit_dataframe
# This will add the column 'early_customer_id' to transit_dataframe
transit_dataframe = transit_dataframe.merge(df1[['early_customer_id', 'similar_reference_customer_id']],
                                            left_on='customer_id', right_on='similar_reference_customer_id',
                                            how='left')

# Step 3: Replace 'customer_id' in transit_dataframe with 'early_customer_id'
transit_dataframe['customer_id'] = transit_dataframe['early_customer_id']

# Optional: Drop the now redundant columns if they are not needed
transit_dataframe.drop(columns=['Unnamed: 0','early_customer_id', 'similar_reference_customer_id'], inplace=True)

readyToRun = pd.concat([df2, transit_dataframe], ignore_index=True)

print(len(readyToRun['customer_id']))
print(len(readyToRun['customer_id'].unique()))

#
# directory = 'readyForBGNBDKModelRun'
#
# # Check if the directory exists, and if not, create it
# if not os.path.exists(directory):
#     os.makedirs(directory)
#
# # Specify the path of the new CSV file
# file_path = os.path.join(directory, 'readyToRun198features.csv')
#
# # Save the dataframe to a CSV file
# readyToRun.to_csv(file_path, index=False)