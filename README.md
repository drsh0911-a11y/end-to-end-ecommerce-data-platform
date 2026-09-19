# 🛒 End-to-End E-Commerce Data Platform

An enterprise-grade, modern data platform built using a Medallion Architecture (Bronze, Silver, Gold), orchestrated via Apache Airflow, processed with PySpark, modeled using dbt, and provisioned with Terraform.

---

## 🏗️ Architecture Flow
```mermaid
graph LR
    A[Raw E-Commerce Sources] -->|PySpark Ingestion| B[(Bronze Layer: Parquet)]
    B -->|Cleaning & Transformation| C[(Silver Layer: Partitioned)]
    C -->|Snowflake Loader| D[(Snowflake Warehouse)]
    D -->|dbt Build & Tests| E[(Gold Layer: Star Schema / Marts)]
    F[Apache Airflow] -->|Orchestrates All DAGs| A
    F -->|Orchestrates| C
    F -->|Orchestrates| D
