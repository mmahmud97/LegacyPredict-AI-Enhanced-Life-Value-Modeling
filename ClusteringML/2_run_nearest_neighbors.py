import pandas as pd
from sklearn.decomposition import PCA
from sklearn.neighbors import NearestNeighbors
from datetime import datetime

# Load the data

#this is data that has customer ids for early customers
df1 = pd.read_csv("FilesCreatedMidProcess/Processed_Dataset.csv")

print(df1.columns)

#this is data that has customer ids for early customers
df2 = pd.read_csv(
    "../0_Start Files - Extract from SQL/EarlyCustomers_From_RetinaAI_StagingArea_clv_orders_Customer_IDs_Only.csv")

# Create a list of customer_ids from df2 for comparison
customer_ids_df2 = df2['customer_id'].tolist()

# Split df1 into 'early' and 'reference'
early = df1[df1['customer_id'].isin(customer_ids_df2)]
reference = df1[~df1['customer_id'].isin(customer_ids_df2)]

# Save the 'customer_id' from both dataframes
early_customer_ids = early['customer_id']
reference_customer_ids = reference['customer_id']

# Drop the 'Unnamed: 0' and 'customer_id' columns from both dataframes
early.drop(columns=['Unnamed: 0', 'customer_id'], inplace=True)
reference.drop(columns=['Unnamed: 0', 'customer_id'], inplace=True)

# Reduce the dimensionality with PCA
pca = PCA(n_components=100)  # Adjust n_components based on the explained variance
early_pca = pca.fit_transform(early)
reference_pca = pca.transform(reference)

# Initialize NearestNeighbors
nn = NearestNeighbors(n_neighbors=1, algorithm='ball_tree')
nn.fit(reference_pca)

# Define the batch size
batch_size = 10000  # Adjust this based on your available memory

# Process in batches
results_list = []
for i in range(0, early_pca.shape[0], batch_size):
    batch_end = min(i + batch_size, early_pca.shape[0])
    early_pca_batch = early_pca[i:batch_end]
    distances, indices = nn.kneighbors(early_pca_batch)
    similar_indices = indices.flatten()
    similar_customer_ids_batch = reference_customer_ids.iloc[similar_indices].values
    results_batch = pd.DataFrame({
        'early_customer_id': early_customer_ids[i:batch_end].values,
        'similar_reference_customer_id': similar_customer_ids_batch
    })
    results_list.append(results_batch)
    print(i)

# Concatenate all results
final_results = pd.concat(results_list, ignore_index=True)

current_time = datetime.now()
formatted_time = current_time.strftime("%Y%m%d_%H%M%S")
filename = f"ResultFiles/matched_customers_nearest_neighbor_{formatted_time}.csv"
final_results.to_csv(filename, index=False)

# Output the head of the results for verification
print(final_results.head())