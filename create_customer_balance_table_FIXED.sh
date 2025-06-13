#!/bin/bash

# Set environment variables
PROJECT_ID="calcium-field-455700-p2"
DATASET="customer_data"
TABLE_NAME="customer_account_balance"

# Create the BigQuery table (one-time setup)
echo "Creating BigQuery table: ${DATASET}.${TABLE_NAME}..."

bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" <<EOF
CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DATASET}.${TABLE_NAME}\` (
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  total_balance NUMERIC,
  last_updated TIMESTAMP
)
OPTIONS(
  description="Customer Account Balance Table",
  labels=[("env", "prod")]
);
EOF

echo "Table creation completed."
