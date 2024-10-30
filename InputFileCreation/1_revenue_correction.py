import pandas as pd

def revenue_correction(df):
    print("Starting revenue correction...")
    # Identify and drop credit memo values and their corresponding positive values
    temp_df = df[['customer_id', 'order_id', 'order_revenue']].copy()
    temp_df['negative_revenue'] = temp_df['order_revenue'] < 0
    temp_df['abs_revenue'] = temp_df['order_revenue'].abs()
    temp_df.sort_values(by=['customer_id', 'abs_revenue'], inplace=True)

    temp_df['paired'] = temp_df['negative_revenue'] != temp_df['negative_revenue'].shift(-1)
    temp_df['paired'] &= temp_df['abs_revenue'] == temp_df['abs_revenue'].shift(-1)
    temp_df['paired'] &= temp_df['customer_id'] == temp_df['customer_id'].shift(-1)
    drop_indices = temp_df[temp_df['paired']].index.union(temp_df[temp_df['paired']].index + 1)
    df = df.drop(drop_indices)

    # Convert other negative values to +1
    df['order_revenue'] = df['order_revenue'].apply(lambda x: 1 if x < 0 else x)

    print("Revenue correction complete.")
    return df

# Example usage
df = pd.read_csv("Mid_Process_transit_File_Repository/0_import_orders.csv")
corrected_df = revenue_correction(df)

print("Length of dataframe: ", len(df['customer_id'].unique()))
print("Length of dataframe: ", len(corrected_df['customer_id'].unique()))
print("Values dropped: ", len(df['customer_id'].unique()) - len(corrected_df['customer_id'].unique()))

corrected_df.to_csv("Mid_Process_transit_File_Repository/1_revenue_corrected_import_orders.csv")

print("File: 1_revenue_correction.py")
print("Run: Successful")