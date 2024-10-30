import pandas as pd
from sklearn.decomposition import PCA
from sklearn.cluster import MiniBatchKMeans
from datetime import datetime

# Load the data
df1 = pd.read_csv("../FilesCreatedMidProcess/Processed_Dataset.csv")
df2 = pd.read_csv(
    "../../0_Start Files - Extract from SQL/EarlyCustomers_From_RetinaAI_StagingArea_clv_orders_Customer_IDs_Only.csv")

# Extract customer IDs for early customers
early_customer_ids = set(df2['customer_id'])

# Split df1 into 'early' and 'reference' using explicit copies
early = df1[df1['customer_id'].isin(early_customer_ids)].copy()
reference = df1[~df1['customer_id'].isin(early_customer_ids)].copy()

# Drop unnecessary columns using inplace=True
early.drop(columns=['Unnamed: 0', 'customer_id'], inplace=True)
reference.drop(columns=['Unnamed: 0', 'customer_id'], inplace=True)

# Reduce dimensionality with PCA
pca = PCA(n_components=100)
early_pca = pca.fit_transform(early)
reference_pca = pca.transform(reference)
print("Success pca")

# Perform MiniBatchKMeans clustering on the reference data
num_clusters = 5  # Example number of clusters, adjust as needed
kmeans = MiniBatchKMeans(n_clusters=num_clusters, random_state=42)
kmeans.fit(reference_pca)
print("Success k means")


# Assign cluster labels to reference and early data
reference['Cluster'] = kmeans.predict(reference_pca)
print("Success ref cluster")
early['Cluster'] = kmeans.predict(early_pca)
print("Success erl cluster")

# Merge early and reference based on cluster assignments
merged_df = pd.merge(early, reference[['customer_id', 'Cluster']], on='Cluster', how='left', suffixes=('_early', '_reference'))
print("Success merge")
# Extract matched customer IDs
final_results = merged_df[['customer_id_early', 'customer_id_reference']]

# Save results to a CSV file
formatted_time = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"ResultFiles/matched_customers_minibatchkmeans_{formatted_time}.csv"
final_results.to_csv(filename, index=False)

# Output the head of the results for verification
print(final_results.head())
