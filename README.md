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
 We used Cloud Scheduler as the core orchestrator to trigger each stage of the pipeline at specific times. This created a reliable, hands-free data flow that refreshes the system daily without any manual steps. The pipeline follows the bronze–silver–gold architecture, which is widely used in real-world data engineering projects.

Step 1: Ingestion to Raw (Bronze Layer)
The first Cloud Scheduler job runs at 7:00 AM and triggers a Cloud Function that copies a .csv file from a backend GCS bucket to our raw/bronze bucket. This step ensures that the raw data is securely stored and available for processing.

We use Python code inside the Cloud Function to call GCP’s Storage API.

This function acts as a gatekeeper for our data pipeline, verifying that the correct file is copied before the transformation begins.

Running this as a scheduled job helps us build a repeatable, time-driven ingestion mechanism.

Step 2: Transformation to Curated (Silver Layer)
At 7:05 AM, a second Cloud Scheduler job submits a Dataproc PySpark job. The PySpark script reads the raw .csv file from the bronze layer, performs data cleaning and validation, and then writes the cleaned data as Parquet files into the curated (silver) bucket.

This transformation step includes:

Removing null values

Renaming and standardizing column names

Converting formats (e.g., dates or numbers)

Ensuring schema consistency

This stage is essential because only clean and validated data should move to the analytics layer. By scheduling it after the ingestion job, we make sure we’re always processing fresh and updated data.

Step 3: Load to BigQuery (Gold Layer)
At 7:15 AM, a third Cloud Scheduler job triggers another Cloud Function that loads the curated Parquet files into a BigQuery table. This is the final stage of the pipeline—also called the gold layer—which makes the data ready for analytics, reporting, or dashboarding.

The load is done using the BigQuery Python client and load_table_from_uri().

This step either appends or replaces data in the BigQuery table, depending on the requirement.

Once this job completes, decision-makers and analysts have access to up-to-date, clean, and trusted data.

The entire data pipeline is orchestrated using Airflow Composer, enabling seamless end-to-end automation of all steps from ingestion to BigQuery upsert.


 
