import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, LabelEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# Load dataset
df = pd.read_csv(
    "../2_Create customers_model_features/Result customers_model_features_1/1_customers_model_features_ready_for_early_model_imputation.csv")
print(df.columns)
print(df.shape)

# Drop the 'member_segment' column
df.drop(columns=['member_segment'], inplace=True)

# Define columns based on the imputation and encoding strategy
columns_to_impute_most_frequent = ['position_level_standard_job_title', 'industry_category', 'is_free',
                                   'gender', 'hr_department_size', 'degree_type', 'membership_type',
                                   'membership_subtype', 'customer_segment', 'membership_status',
                                   'first_product_category', 'customer_type', 'company_size', 'job_function']

columns_to_impute_median = ['num_free', 'first_basket_size', 'package_count', 'first_order_value', 'first_order_discount_value']

columns_no_imputation = ['is_bundle', 'num_subscription', 'num_bundle']

# Imputation and Encoding
imputer_most_frequent = SimpleImputer(strategy='most_frequent')
df[columns_to_impute_most_frequent] = imputer_most_frequent.fit_transform(df[columns_to_impute_most_frequent])

imputer_median = SimpleImputer(strategy='median')
df[columns_to_impute_median] = imputer_median.fit_transform(df[columns_to_impute_median])

# Perform Linearity Checks Here
# For example, pairplot or correlation matrix

print("one hot encoding")
# One-Hot Encoding
onehot_columns = columns_to_impute_most_frequent + columns_no_imputation
df = pd.get_dummies(df, columns=onehot_columns, drop_first=True)

print("label encoding")
# Label Encoding
label_encoder = LabelEncoder()
df['first_product'] = df['first_product'].fillna('Unknown')
df['first_product'] = label_encoder.fit_transform(df['first_product'])

df['certification_status'] = df['certification_status'].fillna('Unknown')
df['certification_status'] = label_encoder.fit_transform(df['certification_status'])

df['job_title'] = df['job_title'].fillna('Unknown')
df['job_title'] = label_encoder.fit_transform(df['job_title'])

# Standardization of numerical columns
num_cols = ['first_order_value', 'num_free', 'first_basket_size', 'package_count']
scaler = StandardScaler()
df[num_cols] = scaler.fit_transform(df[num_cols])

print("starting final save")
# Saving the processed dataset
df.to_csv("FilesCreatedMidProcess/Processed_Dataset.csv", index=False)
