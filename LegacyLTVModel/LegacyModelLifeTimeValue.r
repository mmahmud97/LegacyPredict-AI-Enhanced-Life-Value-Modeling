# Load essential libraries, install if missing
if (!require("DBI")) install.packages("DBI")
if (!require("odbc")) install.packages("odbc")
library(DBI)
library(odbc)

# Define SQL Server Connection Settings
sql_server <- "SERVER_PLACEHOLDER"
db_name <- "DATABASE_PLACEHOLDER"
username <- "USER_PLACEHOLDER"
password <- "PASS_PLACEHOLDER"
port_number <- 1433 # Correct port configuration

# Establish SQL Server Connection
db_connect <- function(server, database, uid, pwd, port, timeout = 45) {
  conn <- tryCatch({
    dbConnect(
      odbc(),
      Driver = "ODBC Driver 17 for SQL Server",
      Server = server,
      Database = database,
      UID = uid,
      PWD = pwd,
      Port = port,
      timeout = timeout
    )
  }, error = function(e) {
    stop("Error establishing database connection: ", e$message)
  })
  if (!is.null(conn) && dbIsValid(conn)) {
    message("Database connection successful.")
  } else {
    stop("Database connection failed.")
  }
  return(conn)
}

# Initialize connection
conn <- db_connect(sql_server, db_name, username, password, port_number)

# Function to fetch data with error-handling
fetch_sql_data <- function(connection, query) {
  tryCatch({
    result <- dbGetQuery(connection, query)
    if (nrow(result) > 0) {
      print(head(result))
    } else {
      message("Query returned no results.")
    }
    return(result)
  }, error = function(e) {
    stop("Error executing query: ", e$message)
  })
}

# SQL Query for extracting data
sql_query <- "
  SELECT [order_id], [cust_code], [order_timestamp], 
         [order_value], [gross_margin], [discount_value]
  FROM [NewSchema].[DataWarehouse].[OrdersData]
  WHERE order_value > 0
"
order_data <- fetch_sql_data(conn, sql_query)

# Disconnect after fetching data
dbDisconnect(conn)
message("SQL data retrieval complete and connection closed.")

# Write results to CSV for further analysis
write.csv(order_data, "order_data_export.csv", row.names = FALSE)
message("Data written to 'order_data_export.csv'.")

# Load and inspect data for manipulation
library(readr)
loaded_data <- read_csv("order_data_export.csv")
message("Loaded Data:")
print(head(loaded_data))

# Function to validate data
validate_data <- function(data) {
  if (nrow(data) == 0) stop("Error: No data loaded.")
  if (any(is.na(data))) message("Warning: Missing data detected.")
  message("Data validation complete. Row count: ", nrow(data))
}

validate_data(loaded_data)

# Install dependencies
options(repos = c(CRAN = "https://packagemanager.posit.co/cran/latest"))
if (!require("devtools")) install.packages("devtools")
devtools::install_github("https://github.com/r-lib/remotes", dependencies = TRUE, upgrade = "never")

# Load required libraries for manipulation
library(dplyr)
library(BTYDplus)

# Function for preprocessing data
preprocess_order_data <- function(data) {
  data <- data %>%
    mutate(
      purchase_date = as.Date(order_timestamp),
      revenue = as.numeric(order_value)
    ) %>%
    select(-order_id, -order_timestamp, -gross_margin, -discount_value)
  
  data <- data %>% filter(revenue > 0)
  if (any(data$revenue <= 0)) stop("Preprocessing failed. Non-positive revenue found.")
  
  return(data)
}

# Apply preprocessing
customer_data <- preprocess_order_data(loaded_data)
print("Customer Data:")
print(head(customer_data))

# CLV Model Parameter Initialization
default_params <- c(r = 9.5, alpha = 4200, a = 0.4, b = 1.5)
predict_years <- c(1, 3, 5, 7, 10)
max_attempts <- 10
sample_size <- 100000

# Aggregation Function for CLV Analysis
aggregate_for_model <- function(data) {
  aggregated <- BTYDplus::elog2cbs(data, units = "day")
  if (nrow(aggregated) == 0) stop("Aggregation failed. No records.")
  return(aggregated)
}

# Aggregate data for model
aggregated_data <- aggregate_for_model(customer_data)
print("Aggregated Data:")
print(head(aggregated_data))

# Estimate Parameters
estimate_model_params <- function(agg_data, start_params, k = 1) {
  params <- BTYDplus::mbgcnbd.EstimateParameters(agg_data, k = k, par.start = start_params)
  print("Estimated Parameters:")
  print(params)
  return(params)
}

estimated_params <- estimate_model_params(aggregated_data, default_params)

# Calculate Probability of Being Active
aggregated_data$prob_alive <- BTYDplus::mbgcnbd.PAlive(
  estimated_params,
  aggregated_data$x,
  aggregated_data$t.x,
  aggregated_data$T.cal
)

# Helper function for creating column names
generate_column_name <- function(year, prefix = "", suffix = "", pad = TRUE) {
  stopifnot(length(year) == 1)
  if (year < 0) stop("Invalid year value.")
  
  unit_str <- ifelse(year %% 1 == 0, "yr", "mo")
  padded_val <- ifelse(pad, sprintf("%02d", year), as.character(year))
  
  final_name <- paste0(prefix, padded_val, unit_str, suffix)
  return(final_name)
}

# Future Predictions for Multiple Timeframes
for (year in predict_years) {
  future_days <- year * 365
  aggregated_data$future_days_var <- future_days - aggregated_data$T.cal
  aggregated_data$future_days_var[aggregated_data$future_days_var < 0] <- 0

  future_orders_col <- generate_column_name(year, "future_orders_")
  aggregated_data[[future_orders_col]] <- BTYDplus::mbgcnbd.ConditionalExpectedTransactions(
    params = estimated_params,
    T.star = aggregated_data$future_days_var,
    x = aggregated_data$x,
    t.x = aggregated_data$t.x,
    T.cal = aggregated_data$T.cal
  )
  
  # Address infinite predictions due to model convergence issues
  aggregated_data[[future_orders_col]][is.infinite(aggregated_data[[future_orders_col]])] <- 0
}

# Prepare Final Output DataFrame
output_data <- aggregated_data %>%
  rename(CustomerID = cust_code, FirstPurchase = purchase_date) %>%
  mutate(
    Total_CLV_20yr = revenue + aggregated_data$future_orders_20,
    ChurnProb = round(1 - prob_alive, 2)
  ) %>%
  select(CustomerID, FirstPurchase, ChurnProb, Total_CLV_20yr)

# Customer Segmentation by CLV
output_data <- output_data %>%
  mutate(
    CLV_Segment = case_when(
      Total_CLV_20yr >= quantile(Total_CLV_20yr, 0.9) ~ "Premium",
      Total_CLV_20yr >= quantile(Total_CLV_20yr, 0.7) ~ "High Value",
      Total_CLV_20yr >= quantile(Total_CLV_20yr, 0.5) ~ "Medium Value",
      TRUE ~ "Low Value"
    )
  )

print("Segmented Output:")
print(head(output_data))

# Export Final Data to CSV
final_output <- "clv_segmented_data.csv"
write_csv(output_data, final_output)
message(paste("Segmented data exported to", final_output))

# Database Reconnection for Upload
conn <- db_connect(sql_server, db_name, username, password, port_number)

# Dynamic Table Name with Date
new_table <- paste0("Customer_Segment_", Sys.Date())
table_name_bracketed <- sprintf("[dbo].[%s]", new_table)

# Verify Table Exists; Truncate if Exists, or Create
dbExecute(conn, sprintf(
  "IF OBJECT_ID('%s', 'U') IS NOT NULL TRUNCATE TABLE %s",
  table_name_bracketed, table_name_bracketed
))

# Create Table if it does not exist
dbExecute(conn, sprintf(
  "IF OBJECT_ID('%s', 'U') IS NULL BEGIN CREATE TABLE %s (
    CustomerID int, 
    FirstPurchase date, 
    ChurnProb float, 
    Total_CLV float, 
    CLV_Segment varchar(255)
  ) END",
  table_name_bracketed, table_name_bracketed
))

# Close connection after completion
dbDisconnect(conn)
message(paste("Data successfully uploaded to SQL table:", new_table))
