'''Batch Processing the commented out code below'''
import pandas as pd
from sklearn.decomposition import PCA
from scipy.cluster.hierarchy import fcluster, linkage
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
pca = PCA(n_components=50)  # Reduce number of components
early_pca = pca.fit_transform(early)
reference_pca = pca.transform(reference)

# Perform hierarchical clustering on the entire reference data
Z = linkage(reference_pca, method='ward')

# Assign clusters to reference customers
num_clusters = 5  # Example number of clusters, adjust as needed
reference_clusters = fcluster(Z, t=num_clusters, criterion='maxclust')

# Ensure that the length of reference clusters matches the length of the reference DataFrame
assert len(reference_clusters) == len(reference), "Length of reference clusters does not match length of reference DataFrame"

# Assign clusters to the 'reference' DataFrame
reference['Cluster'] = reference_clusters

# Assign clusters to early customers based on reference clusters
early_clusters = fcluster(Z, t=num_clusters, criterion='maxclust', distances=pca.transform(early)[:, :num_clusters])

# Ensure that the length of early clusters matches the length of the early DataFrame
assert len(early_clusters) == len(early), "Length of early clusters does not match length of early DataFrame"

# Assign clusters to the 'early' DataFrame
early['Cluster'] = early_clusters

# Merge early and reference based on cluster assignments
merged_df = pd.merge(early, reference[['customer_id', 'Cluster']], on='Cluster', how='left', suffixes=('_early', '_reference'))

# Extract matched customer IDs
final_results = merged_df[['customer_id_early', 'customer_id_reference']]

# Save results to a CSV file
formatted_time = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"ResultFiles/matched_customers_{formatted_time}.csv"
final_results.to_csv(filename, index=False)

# Output the head of the results for verification
print(final_results.head())



'''import pandas as pd
from sklearn.decomposition import PCA
from scipy.cluster.hierarchy import fcluster, linkage
from datetime import datetime

# Load the data
df1 = pd.read_csv("FilesCreatedMidProcess/Processed_Dataset.csv")
df2 = pd.read_csv("../0_Start Files - Extract from SQL/EarlyCustomers_From_RetinaAI_StagingArea_clv_orders_Customer_IDs_Only.csv")
print("Data Loaded")

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

# Perform hierarchical clustering on the reference data
Z = linkage(reference_pca, method='ward')

# Assign clusters to reference customers
num_clusters = 5  # Example number of clusters, adjust as needed
reference['Cluster'] = fcluster(Z, t=num_clusters, criterion='maxclust')

# Assign clusters to early customers based on reference clusters
early['Cluster'] = fcluster(Z, t=num_clusters, criterion='maxclust',
                            distances=pca.transform(early)[:, :num_clusters])

# Merge early and reference based on cluster assignments
merged_df = pd.merge(early, reference[['customer_id', 'Cluster']],
                     on='Cluster', how='left', suffixes=('_early', '_reference'))

# Extract matched customer IDs
final_results = merged_df[['customer_id_early', 'customer_id_reference']]

# Save results to a CSV file
formatted_time = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"ResultFiles/matched_customers_{formatted_time}.csv"
final_results.to_csv(filename, index=False)

# Output the head of the results for verification
print(final_results.head())
'''



