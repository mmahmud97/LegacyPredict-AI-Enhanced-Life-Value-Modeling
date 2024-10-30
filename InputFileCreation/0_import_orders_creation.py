#Create the intial import_orders file without negative revenue correction and credit memo correction
import pandas as pd
import os

def create_directory_and_save_file(directory_name, file_name, dataframe):
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    file_path = os.path.join(directory_name, file_name)
    dataframe.to_csv(file_path, index=False)
    print(f"Dataframe saved to '{file_path}'.")

df3 = pd.read_csv("../0_Start Files - Extract from SQL/RetinaAI_StagingArea_clv_orders.csv")
df3 = df3.drop(['is_gift_card_used', 'total_tip_received'], axis=1)

df3['order_timestamp'] = pd.to_datetime(df3['order_timestamp'])
df3['order_date'] = df3['order_timestamp'].dt.date

directory_name = "Mid_Process_Transit_File_Repository"
file_name = "0_import_orders.csv"

print("Length of dataframe: ", len(df3['customer_id'].unique()))

create_directory_and_save_file(directory_name, file_name, df3)
print("File: 0_import_orders_creation.py")
print("Run: Successful")