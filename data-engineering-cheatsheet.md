# Data Engineering Cheat Sheet

A quick-reference guide covering the core tools of a modern data engineering stack: SQL, Python (Pandas/PySpark), orchestration (Airflow), transformation (dbt), containerization (Docker), and cloud warehouses (Databricks & Snowflake).

---

## 1. SQL Essentials

### Window Functions
```sql
-- Rank within a partition
SELECT
    customer_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn,
    RANK()       OVER (PARTITION BY customer_id ORDER BY amount DESC)     AS amount_rank,
    SUM(amount)  OVER (PARTITION BY customer_id ORDER BY order_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    LAG(amount)  OVER (PARTITION BY customer_id ORDER BY order_date)     AS prev_amount
FROM orders;
```

### CTEs & Recursive Queries
```sql
WITH monthly_sales AS (
    SELECT DATE_TRUNC('month', order_date) AS month, SUM(amount) AS total
    FROM orders
    GROUP BY 1
)
SELECT month, total,
       total - LAG(total) OVER (ORDER BY month) AS mom_change
FROM monthly_sales;
```

### Upsert (MERGE) — used constantly in ETL
```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.value = s.value, t.updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (id, value, updated_at) VALUES (s.id, s.value, CURRENT_TIMESTAMP);
```

### Data Quality Sniffs
```sql
-- Duplicate check
SELECT id, COUNT(*) FROM my_table GROUP BY id HAVING COUNT(*) > 1;

-- Null rate by column (Postgres/Snowflake/Databricks SQL)
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) - COUNT(email) AS null_emails,
    ROUND(100.0 * (COUNT(*) - COUNT(email)) / COUNT(*), 2) AS null_pct
FROM customers;
```

---

## 2. Python for Data Engineering

### Pandas — quick data profiling
```python
import pandas as pd

df = pd.read_csv("data/raw/orders.csv")
df.info()
df.describe(include="all")
df.isnull().mean().sort_values(ascending=False)   # null % per column
df.duplicated(subset=["order_id"]).sum()
```

### Pandas — chunked reads for large files
```python
for chunk in pd.read_csv("huge_file.csv", chunksize=100_000):
    process(chunk)
```

### PySpark — DataFrame basics
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("etl").getOrCreate()

df = spark.read.option("header", True).csv("s3://bucket/raw/orders/")

df = (
    df.withColumn("amount", F.col("amount").cast("double"))
      .withColumn("order_date", F.to_date("order_date"))
      .dropDuplicates(["order_id"])
      .filter(F.col("amount") > 0)
)

df.groupBy("category").agg(
    F.sum("amount").alias("total_sales"),
    F.count("*").alias("n_orders"),
).orderBy(F.desc("total_sales")).show()
```

### PySpark — Delta Lake (Databricks)
```python
# Write as Delta (Bronze layer)
df.write.format("delta").mode("overwrite").save("/mnt/bronze/orders")

# Upsert with MERGE
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/mnt/silver/orders")
target.alias("t").merge(
    df.alias("s"), "t.order_id = s.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Time travel
spark.read.format("delta").option("versionAsOf", 3).load("/mnt/silver/orders")
```

---

## 3. Airflow

### Minimal DAG
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "majd",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_orders_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "orders"],
) as dag:

    def extract(**kwargs): ...
    def transform(**kwargs): ...
    def load(**kwargs): ...

    t1 = PythonOperator(task_id="extract", python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load", python_callable=load)

    t1 >> t2 >> t3
```

### Useful CLI commands
```bash
airflow db init
airflow dags list
airflow dags trigger etl_orders_pipeline
airflow tasks test etl_orders_pipeline extract 2026-01-01
```

---

## 4. dbt

### Project structure
```
models/
├── staging/
│   └── stg_orders.sql
├── intermediate/
│   └── int_orders_enriched.sql
└── marts/
    └── fct_orders.sql
```

### Staging model
```sql
-- models/staging/stg_orders.sql
SELECT
    order_id,
    customer_id,
    CAST(order_date AS DATE) AS order_date,
    CAST(amount AS NUMERIC(10,2)) AS amount
FROM {{ source('raw', 'orders') }}
WHERE order_id IS NOT NULL
```

### Fact model with ref()
```sql
-- models/marts/fct_orders.sql
SELECT
    o.order_id,
    o.customer_id,
    d.category,
    o.amount,
    o.order_date
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('dim_products') }} d ON o.product_id = d.product_id
```

### Tests (schema.yml)
```yaml
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: amount
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
```

### CLI
```bash
dbt run
dbt test
dbt docs generate && dbt docs serve
dbt run --select fct_orders+     # model + downstream dependents
```

---

## 5. Docker / Docker Compose (local pipeline stack)

```yaml
# docker-compose.yml — minimal Airflow + Postgres stack
version: "3.8"
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow

  airflow-webserver:
    image: apache/airflow:2.9.0
    depends_on: [postgres]
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./dags:/opt/airflow/dags
    ports:
      - "8080:8080"
    command: webserver
```

```bash
docker compose up -d
docker compose logs -f airflow-webserver
docker compose down -v
```

---

## 6. Databricks

```python
# Databricks notebook — widgets for parameterization
dbutils.widgets.text("run_date", "2026-01-01")
run_date = dbutils.widgets.get("run_date")

# Read/write to Unity Catalog table
df = spark.table("main.bronze.orders")
df.write.mode("append").saveAsTable("main.silver.orders")
```

```bash
# Databricks CLI
databricks workspace import ./notebook.py /Repos/majd/etl/notebook.py
databricks jobs run-now --job-id 12345
```

---

## 7. Snowflake

```sql
-- Warehouse & database setup
CREATE WAREHOUSE IF NOT EXISTS etl_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60;
CREATE DATABASE IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS analytics.marts;

-- Load from stage (S3/external stage)
COPY INTO analytics.raw.orders
FROM @my_stage/orders/
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- Streams + Tasks (Snowflake's native CDC/orchestration)
CREATE OR REPLACE STREAM orders_stream ON TABLE analytics.raw.orders;

CREATE OR REPLACE TASK refresh_fct_orders
  WAREHOUSE = etl_wh
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
  MERGE INTO analytics.marts.fct_orders t
  USING orders_stream s ON t.order_id = s.order_id
  WHEN MATCHED THEN UPDATE SET t.amount = s.amount
  WHEN NOT MATCHED THEN INSERT (order_id, amount) VALUES (s.order_id, s.amount);
```

---

## 8. Git Workflow for Data Projects

```bash
# Sensible .gitignore for data projects
echo "data/raw/\ndata/processed/\n*.parquet\n.env\n__pycache__/\n.venv/" >> .gitignore

# Conventional commits (recruiter-readable history)
git commit -m "feat(ingestion): add incremental extraction for orders API"
git commit -m "fix(dbt): correct null handling in stg_orders"
git commit -m "test(dq): add great_expectations suite for customers table"
```

---

## Quick Reference: When to Use What

| Need                          | Tool                          |
|-------------------------------|--------------------------------|
| Batch orchestration            | Airflow                       |
| SQL-based transformation       | dbt                            |
| Large-scale distributed compute| PySpark / Databricks           |
| Cloud warehouse (SQL-first)    | Snowflake                      |
| Data quality testing           | dbt tests / Great Expectations |
| Local dev environment          | Docker Compose                 |
| Format for analytics tables    | Delta Lake / Parquet + Iceberg |
