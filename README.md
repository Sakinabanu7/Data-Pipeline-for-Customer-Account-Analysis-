 # 💡 Data Pipeline for Customer Account Analysis

This project demonstrates an end-to-end **data pipeline** on **Google Cloud Platform (GCP)** to process, clean, transform, and analyze customer account data. It includes data ingestion, PySpark processing on Dataproc, and upserts into **BigQuery**.

---

## 📌 Project Objectives

- Automate customer data ingestion and transformation.
- Standardize data quality across all stages.
- Enable analytical reporting via BigQuery integration.

---

## 🧱 Architecture Overview

![Architecture Diagram] in the file attachment.

---

## 🔄 Pipeline Steps

### 🟤 Step 1: Raw Data Ingestion (Bronze Layer)
- 📆 Used Cloud Scheduler to copy files from backend bucket to raw bucket on a schedule.

- ✅ Copied 5 CSV files using `gsutil` from backend bucket to raw bucket.

```bash
gsutil cp gs://backend-team-bucket/*.csv gs://bronze-data-pipeline-455700/
````

---

### ⚙️ Step 2: Cleaning & Transformation (Silver Layer)

* 🚀 Tool: PySpark on Dataproc
* 🧼 Cleaned 5 datasets: `accounts.csv`, `customers.csv`, `loans.csv`, `loan_payments.csv`, `transactions.csv`
* ❗ Schema explicitly defined (inferSchema = false)
* 🧾 Removed nulls, filtered invalid records (e.g., negative balances)
* 💾 Output written to: `gs://silver-data-pipeline-455700/`
* 📜 Script: `clean_all_files_dataproc.py`

---

### 🟡 Step 3: Join & Aggregation (Gold Layer)

* 👥 Joined `accounts` with `customers`
* ➕ Computed total balance per customer
* 📤 Saved result to: `gs://gold-data-pipeline-455700/customer_account_balance/`
* 📜 Script: `join_and_aggregate.py`

---

### 🟢 Step 4: Load to BigQuery & Upsert

* 🧪 Loaded Gold data into staging table using:

```bash
bq load --source_format=PARQUET \
project_id:customer_data.refined_gold_customer_balance_temp \
gs://gold-data-pipeline-455700/customer_account_balance/*.parquet
```

* 🏗️ Created target table using:

```bash
bash create_customer_balance_table.sh
```

* 🔁 Upserted daily updates using:

 
bash daily_customer_balance_upsert.sh
```

--- Used Cloud Composer (Airflow) to orchestrate the full pipeline

## 🧑‍💻 My Role: Data Cleaning & Transformation Lead

I was responsible for the **data cleaning and transformation stage** using PySpark on Dataproc:

* Defined schemas manually
* Removed nulls and filtered out bad records
* Saved cleaned data to the silver bucket
* Managed the Dataproc cluster lifecycle to reduce costs

This step ensured the data quality and stability necessary for the rest of the pipeline.

---

## 🗂️ Repository Structure

```
├── clean_all_files_dataproc.py
├── join_and_aggregate.py
├── modify_gold_data.py
├── create_customer_balance_table.sh
├── daily_customer_balance_upsert.sh
├── Data Pipeline_Arch.png
├── Project1_pipeline_img.zip
└── Customer_Data_Pipeline_Detailed.docx
```

---

## 📸 Screenshots

All project execution screenshots are stored in:
📁 `Project1_pipeline_img.zip`

---

## 🔍 Dataset Info

| File                | Description                   |
| ------------------- | ----------------------------- |
| `accounts.csv`      | Customer account details      |
| `customers.csv`     | Demographic data of customers |
| `loans.csv`         | Loan contract records         |
| `loan_payments.csv` | Loan payment transactions     |
| `transactions.csv`  | General transaction logs      |

---

## ✅ Outcomes

* ✅ Successfully implemented GCP-based pipeline architecture
* 🧼 Ensured data quality and standardization with PySpark
* 📊 Enabled insights via aggregated BigQuery tables

---

## 🧰 Tech Stack

* Google Cloud Storage (GCS)
* Google Dataproc (PySpark)
* Google BigQuery
* Shell Scripting
* Git & GitHub

---

## 🏁 Final Output

* 🔢 BigQuery Table: `customer_data.customer_account_balance`
* 🧪 Staging Table: `refined_gold_customer_balance_temp`

```
## Note
☁️ Cloud Scheduler is used to automatically copy files from the backend bucket to the raw/bronze bucket on a daily or scheduled basis.

🔁 The entire data pipeline is orchestrated using Airflow Composer, enabling seamless end-to-end automation of all steps from ingestion to BigQuery upsert.


 
