import os
import pandas as pd

def drop_whale_enterprise(df, enterprise_customers, whale_customers):
    print("Dropping Enterprise and Whale customers...")

    # Get unique customer IDs from enterprise_customers and whale_customers
    excluded_customers = pd.concat([enterprise_customers['customer_id'], whale_customers['customer_id']]).unique()

    # Drop rows where customer ID is in excluded_customers
    df_dropped = df[~df['customer_id'].isin(excluded_customers)]

    return df_dropped

def create_directory_and_save_file(directory_name, file_name, dataframe):
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    file_path = os.path.join(directory_name, file_name)
    dataframe.to_csv(file_path, index=False)
    print(f"Dataframe saved to '{file_path}'.")

# Example usage
df = pd.read_csv("Mid_Process_transit_File_Repository/1_revenue_corrected_import_orders.csv")
df2 = pd.read_csv("../0_Start Files - Extract from SQL/Enterprise.csv")
df3 = pd.read_csv("../0_Start Files - Extract from SQL/Whales.csv")

# Check for customer IDs in df2 that do not exist in df
missing_customers_df2 = df2[~df2['customer_id'].isin(df['customer_id'])]
missing_customers_df3 = df3[~df3['customer_id'].isin(df['customer_id'])]
overlap_enterprise_whale = df2[~df2['customer_id'].isin(df3['customer_id'])]
print(len(overlap_enterprise_whale))
print("Length of main file before removal of enterprise and whales: ", len(df))
print("Length of main file before removal of enterprise and whales: ", len(df['customer_id'].unique()))
print("Length of enterprise file: ", len(df2['customer_id'].unique()))
print("Length of whales file: ", len(df3['customer_id'].unique()))
print("Subtracting counts of Enterprise and Whales from main fail should give us: ", len(df) - len(df2['customer_id'].unique()) - len(df3['customer_id'].unique()))
print("Missing values in df present in df2: ", len(missing_customers_df2))
print("Missing values in df present in df3: ", len(missing_customers_df3))

df_dropped = drop_whale_enterprise(df, df2, df3)

print("length of import dataset after removal of enterprise and whale: ", len(df_dropped))
print("length of import dataset after removal of enterprise and whale: ", len(df_dropped['customer_id'].unique()))

'''Initial preprocessing complete for import_orders.csv'''
'''This file is not ready to be run in the clv code yet'''
'''We need to use this file for earlyCustomersMachineLEarning Model'''

directory_name = "Result_Of_Create_Import_Orders"
file_name = "2_import_orders.csv"
create_directory_and_save_file(directory_name, file_name, df_dropped)

print("File: 2_whale_enterprise_removal.py")
print("Run: Successful")