#!/bin/bash

PROJECT_ID="calcium-field-455700-p2"
DATASET="customer_data"
TARGET_TABLE="customer_account_balance"
STAGING_TABLE="refined_gold_customer_balance_temp"

echo "Performing daily upsert from ${STAGING_TABLE} to ${TARGET_TABLE}..."

bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" <<EOF
MERGE \`${PROJECT_ID}.${DATASET}.${TARGET_TABLE}\` T
USING \`${PROJECT_ID}.${DATASET}.${STAGING_TABLE}\` S
ON T.customer_id = S.customer_id
WHEN MATCHED THEN
  UPDATE SET 
    first_name = S.first_name,
    last_name = S.last_name,
    total_balance = CAST(S.total_balance AS NUMERIC),
    last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (customer_id, first_name, last_name, total_balance, last_updated)
  VALUES (S.customer_id, S.first_name, S.last_name, CAST(S.total_balance AS NUMERIC), CURRENT_TIMESTAMP());
EOF

echo "✅ Daily upsert completed."
