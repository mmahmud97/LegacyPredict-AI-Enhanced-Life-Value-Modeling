import pandas as pd

df1 = pd.read_csv("../2_Create customers_model_features/Mid_Process_Transit_File_Repository/0_customers_model_features.csv", low_memory=False)
order_line_items = pd.read_csv("../0_Start Files - Extract from SQL/RetinaAI_StagingArea_clv_order_line_items.csv", low_memory=False)
orders = pd.read_csv("../0_Start Files - Extract from SQL/RetinaAI_StagingArea_clv_orders.csv",low_memory=False)
print("orders", len(orders['customer_id']))
print("orders", len(orders['customer_id'].unique()))
print("order line items", len(order_line_items['customer_id']))
print("order line items", len(order_line_items['customer_id'].unique()))
# Get unique customer IDs from df1
unique_customer_ids = df1['customer_id'].unique()

# Filter orders DataFrame
orders = orders[orders['customer_id'].isin(unique_customer_ids)]

# Filter order_line_items DataFrame
order_line_items = order_line_items[order_line_items['customer_id'].isin(unique_customer_ids)]

print("customers model features", len(df1))
print("customers model features", len(df1['customer_id'].unique()))
print("orders length: ", len(orders['customer_id']))
print("orders length: ", len(orders['customer_id'].unique()))
print("order line items length: ", len(order_line_items['customer_id']))
print("order line items length: ", len(order_line_items['customer_id'].unique()))

# Convert 'order_timestamp' to datetime
order_line_items['order_timestamp'] = pd.to_datetime(order_line_items['order_timestamp'])

# Sort the DataFrame by customer_id and order_timestamp
sorted_customer_ids = order_line_items.sort_values(by=['customer_id', 'order_timestamp'])

# Drop duplicates to get the earliest order per customer and keep the required columns
first_order_data = sorted_customer_ids.drop_duplicates(subset='customer_id', keep='first')

# Selecting the required columns
first_product_category_first_product = first_order_data[['customer_id', 'product_category', 'product_title']]

# Renaming columns to match the requested output
first_product_category_first_product = first_product_category_first_product.rename(columns={
    'product_category': 'first_product_category',
    'product_title': 'first_product'
})

# Convert 'order_timestamp' to datetime
orders['order_timestamp'] = pd.to_datetime(orders['order_timestamp'])

# Sort the DataFrame by customer_id and order_timestamp
sorted_orders = orders.sort_values(by=['customer_id', 'order_timestamp'])

# Drop duplicates to get the earliest order per customer and explicitly create a copy
first_order_data = sorted_orders.drop_duplicates(subset='customer_id', keep='first').copy()

# Check if the first order had a revenue of 0.0 to determine if it was free
# Using .loc to avoid SettingWithCopyWarning
first_order_data.loc[:, 'is_free'] = first_order_data['order_revenue'].apply(lambda x: x == 0.0).astype(int)

# Group by customer_id and summarize the 'is_free' column
free_count = first_order_data.groupby('customer_id')['is_free'].agg(['sum', 'max'])
free_count.columns = ['num_free', 'is_free']

# Reset index to make customer_id a column
num_free_is_free = free_count.reset_index()

# Check if the product_title contains the word 'bundle'
order_line_items['is_bundle'] = order_line_items['product_title'].apply(lambda x: 'bundle' in x.lower()).astype(int)

# Group by customer_id and count the number of bundles
bundle_count = order_line_items.groupby('customer_id')['is_bundle'].agg(['sum', 'max'])
bundle_count.columns = ['num_bundle', 'is_bundle']

# Reset index to make customer_id a column
num_bundles_is_bundle = bundle_count.reset_index()

# Check if the product_category is 'Subscription' or 'SUBSCRIPTION'
order_line_items['is_subscription'] = order_line_items['product_category'].apply(lambda x: x.lower() == 'subscription').astype(int)

# Group by customer_id and count the number of subscriptions
subscription_count = order_line_items.groupby('customer_id')['is_subscription'].agg(['sum', 'max'])
subscription_count.columns = ['num_subscription', 'is_subscription']

# Reset index to make customer_id a column
num_subscriptions_is_subscription = subscription_count.reset_index()

# Convert 'order_timestamp' to datetime if it's not already
orders['order_timestamp'] = pd.to_datetime(orders['order_timestamp'])

# Sort the data by customer_id and order_timestamp to ensure we get the first order correctly
another_dataset_sorted = orders.sort_values(by=['customer_id', 'order_timestamp'])

# Drop duplicates to get the earliest order per customer and explicitly create a copy
first_order_data = another_dataset_sorted.drop_duplicates(subset='customer_id', keep='first').copy()

# Rename columns to match the requested output
first_order_data.rename(columns={
    'order_revenue': 'first_order_value',
    'order_discount_value': 'first_order_discount_value'
}, inplace=True)

first_order_value_first_order_discount_value = first_order_data[['customer_id', 'first_order_value', 'first_order_discount_value']]

# Convert 'order_timestamp' to datetime if it's not already
order_line_items['order_timestamp'] = pd.to_datetime(order_line_items['order_timestamp'])

# Get the earliest order timestamp for each customer
earliest_order_time = order_line_items.groupby('customer_id')['order_timestamp'].min().reset_index()

# Create the two new columns based on the earliest order timestamp
earliest_order_time['first_order_day_of_week'] = earliest_order_time['order_timestamp'].dt.dayofweek
earliest_order_time['first_order_hour_of_day'] = earliest_order_time['order_timestamp'].dt.hour

# Select only the required columns
first_order_day_of_week_and_hour_of_day = earliest_order_time[['customer_id', 'first_order_day_of_week', 'first_order_hour_of_day']]

orders['order_timestamp'] = pd.to_datetime(orders['order_timestamp'])
sorted_orders = orders.sort_values(by=['customer_id', 'order_timestamp'])
earliest_order_time = sorted_orders.groupby('customer_id')['order_timestamp'].transform('min')
sorted_orders['earliest_order_timestamp'] = earliest_order_time
first_orders = sorted_orders[sorted_orders['order_timestamp'] == sorted_orders['earliest_order_timestamp']]
first_basket_size = first_orders.groupby('customer_id').size()
first_basket_size_package_count = first_basket_size.reset_index(name='first_basket_size')
first_basket_size_package_count['package_count'] = first_basket_size_package_count['first_basket_size']

final_dataframe = first_product_category_first_product

# Merging other dataframes one by one
final_dataframe = pd.merge(final_dataframe, num_free_is_free, on='customer_id', how='left')
final_dataframe = pd.merge(final_dataframe, num_bundles_is_bundle, on='customer_id', how='left')
final_dataframe = pd.merge(final_dataframe, num_subscriptions_is_subscription, on='customer_id', how='left')
final_dataframe = pd.merge(final_dataframe, first_order_value_first_order_discount_value, on='customer_id', how='left')
final_dataframe = pd.merge(final_dataframe, first_order_day_of_week_and_hour_of_day, on='customer_id', how='left')
final_dataframe = pd.merge(final_dataframe, first_basket_size_package_count, on='customer_id', how='left')

# Merge final_dataframe with df1 on 'customer_id'
final_dataframe = pd.merge(final_dataframe, df1, on='customer_id', how='left')

# Define the desirable columns to keep
desirable_columns = [
    'customer_id', 'first_order_day_of_week', 'certification_status',
    'first_order_hour_of_day', 'company_size', 'first_order_value',
    'first_order_discount_value', 'is_subscription', 'job_title', 'job_function',
    'position_level_standard_job_title', 'industry_category', 'is_free', 'is_bundle',
    'gender', 'hr_department_size', 'degree_type', 'membership_type', 'membership_subtype',
    'num_subscription', 'customer_type', 'customer_segment', 'num_free', 'num_bundle',
    'member_segment', 'membership_status', 'first_basket_size', 'package_count',
    'first_product', 'first_product_category'
]

# Filter the final_dataframe to keep only the desirable columns
final_result = final_dataframe[desirable_columns]
print("customer_model_features length: ",len(final_result))
print("customer_model_features length: ",len(final_result['customer_id'].unique()))

#../2_Create customers_model_features/Mid_Process_Transit_File_Repository/
final_result.to_csv("../2_Create customers_model_features/Result customers_model_features_1/1_customers_model_features_ready_for_early_model_imputation.csv")