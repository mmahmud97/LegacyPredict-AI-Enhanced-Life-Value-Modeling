# Load necessary packages and install if missing
if (!require("DBI")) install.packages("DBI")
if (!require("odbc")) install.packages("odbc")
library(DBI)
library(odbc)

# Database connection configuration
db_params <- list(
  server = "YOUR_SERVER",
  database = "YOUR_DATABASE",
  username = "YOUR_USERNAME",
  password = "YOUR_PASSWORD",
  port = 1433
)

# Initialize database connection
db_conn <- dbConnect(odbc(),
                     Driver = "ODBC Driver 17 for SQL Server",
                     Server = db_params$server,
                     Database = db_params$database,
                     UID = db_params$username,
                     PWD = db_params$password,
                     Port = db_params$port,
                     timeout = 30)

# Connection validation
if (dbIsValid(db_conn)) {
  message("Connected to database successfully.")
} else {
  stop("Database connection failed.")
}

# SQL query execution for initial data retrieval
query_results <- dbGetQuery(db_conn, "
  SELECT order_id, customer_id, order_timestamp, 
         order_revenue, order_gross_margin, order_discount_value
  FROM YOUR_SCHEMA.YOUR_TABLE
  WHERE order_revenue > 0
")

# Previewing query results
if (nrow(query_results) > 0) {
  head(query_results)
} else {
  print("No results found.")
}

# Disconnect from database
dbDisconnect(db_conn)
print("Process completed.")

# Export data to CSV
output_file <- "order_data.csv"
write.csv(query_results, output_file, row.names = FALSE)
print("Data exported to CSV.")

# Load CSV data for further analysis
order_data <- read.csv(output_file)
print(head(order_data))

# Check row count
cat("Number of rows:", nrow(order_data), "\n")

# Install additional packages if necessary
if (!require("devtools")) install.packages("devtools")
devtools::install_github("YOUR_GITHUB_REPO",
                         dependencies = TRUE,
                         upgrade = "never",
                         force = TRUE)

# Set up files for data processing
input_csv <- "order_data.csv"
processed_csv <- "calculated_ltv.csv"

# Load data processing libraries
library(tidyverse)

# Import CSV data and process columns
processed_data <- read_csv(input_csv) %>%
  mutate(transaction_date = as.Date(order_timestamp)) %>%
  select(-c(order_id, order_timestamp, order_gross_margin, order_discount_value)) %>%
  rename(customer = customer_id, revenue = order_revenue, date = transaction_date) %>%
  filter(revenue > 0)

print("Initial data processing complete.")
print(head(processed_data))

# Model parameters and horizons
model_params <- list(r = 9.2, alpha = 3338.5, a = 0.47, b = 1.41)
prediction_intervals <- c(1, 2, 3, 5, 10, 20)

# Convert event log to customer-by-statistic format
customer_stats <- BTYDplus::elog2cbs(processed_data, units = "day")
print(head(customer_stats))

# Estimate BG/NBD parameters
params_estimate <- BTYDplus::mbgcnbd.EstimateParameters(customer_stats, par.start = unlist(model_params))
print(params_estimate)

# Compute active probability for customers
customer_stats <- customer_stats %>%
  mutate(active_probability = BTYDplus::mbgcnbd.PAlive(params_estimate, x, t.x, T.cal))

# Predict future transactions for each interval
for (interval in prediction_intervals) {
  interval_days <- interval * 365
  customer_stats <- customer_stats %>%
    mutate(!!paste0("predicted_transactions_", interval, "yr") := BTYDplus::mbgcnbd.ConditionalExpectedTransactions(
      params = params_estimate, T.star = interval_days, x = x, t.x = t.x, T.cal = T.cal
    ))
}

# Save predictions to a new CSV
write_csv(customer_stats, processed_csv)
print("Predictions saved to CSV:", processed_csv)

# Import and refine data for final output
final_data <- read_csv(processed_csv) %>%
  select(customer, revenue, first_purchase, active_probability, predicted_transactions_20yr) %>%
  mutate(
    customer_id = as.integer(customer),
    calculated_clv = round(revenue + predicted_transactions_20yr, 2),
    churn_risk = round(1 - active_probability, 2)
  ) %>%
  select(customer_id, calculated_clv, first_purchase, churn_risk)

# Segment based on CLV value
final_data <- final_data %>%
  mutate(clv_segment = case_when(
    calculated_clv >= quantile(calculated_clv, 0.98) ~ "Premium",
    calculated_clv >= quantile(calculated_clv, 0.80) ~ "High",
    calculated_clv >= quantile(calculated_clv, 0.20) ~ "Standard",
    TRUE ~ "Low"
  ))

# Database reconnection for upload
upload_conn <- dbConnect(odbc(),
                         Driver = "ODBC Driver 17 for SQL Server",
                         Server = db_params$server,
                         Database = db_params$database,
                         UID = db_params$username,
                         PWD = db_params$password,
                         Port = db_params$port,
                         timeout = 30)

# Table name with today's date
table_name <- paste0("Customer_LTV_", Sys.Date())

# Create table if it doesn't exist and insert data
create_statement <- sprintf("
  IF OBJECT_ID('[dbo].[%s]', 'U') IS NULL
  BEGIN 
    CREATE TABLE [dbo].[%s] (
      customer_id int,
      calculated_clv float,
      first_purchase date,
      churn_risk float,
      clv_segment varchar(50)
    )
  END
", table_name, table_name)
dbExecute(upload_conn, create_statement)

# Insert refined data into SQL table
dbWriteTable(upload_conn, name = table_name, value = final_data, append = TRUE, row.names = FALSE)

print("Data upload to SQL table complete:", table_name)
dbDisconnect(upload_conn)