# 🍽️ Enterprise Lakehouse Architecture on Databricks
![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Streaming](https://img.shields.io/badge/Real--Time_Streaming-FF9900?style=for-the-badge)
![Mosaic AI](https://img.shields.io/badge/Mosaic_AI-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python_Env-3776AB?style=for-the-badge&logo=python&logoColor=white)

An end-to-end Data Engineering & Analytics platform built on Databricks Lakehouse, modeling a large UAE-based restaurant chain operating across multiple locations.

This project demonstrates:

- Real-time + batch data ingestion  
- Medallion architecture (Bronze / Silver / Gold)  
- AI-powered SQL transformations  
- Unity Catalog governance  
- Native Databricks AI/BI Dashboards  
- Full pipeline orchestration using Workflows  

---

## 🏗️ Architecture Overview

![Architecture](images/Databrick_project_architecture.png)

Cloud Stack:

- Azure SQL Database → Batch data source  
- Azure Event Hubs (Kafka surface) → Real-time streaming  
- Azure Data Lake Storage (Delta Lake) → Storage layer  
- Databricks Premium Workspace  
  - Unity Catalog  
  - Serverless Compute  
  - Spark Declarative Pipelines  
  - LakeFlow Connect  
  - Mosaic AI Model Serving  
  - Databricks Workflows  
  - Databricks AI/BI Dashboards  

---

# 📊 Project Overview

This project simulates a multi-location UAE restaurant chain and implements a modern Lakehouse architecture capable of:

- Handling streaming POS orders  
- Syncing batch master data  
- Performing CDC ingestion  
- Modeling a Star Schema  
- Running AI sentiment analysis directly in SQL  
- Serving business dashboards natively in Databricks  

---

# 🧱 Medallion Architecture

## 🔹 Bronze Layer (Raw Ingestion)

### Data Sources

Batch:
- Historical Orders  
- Customers  
- Restaurants  
- Menu Items  
- Reviews  
(Source: Azure SQL Database)

Streaming:
- Live POS orders  
(Source: Azure Event Hubs)

### Ingestion Mechanisms

**LakeFlow Connect**
- Ingests Azure SQL tables  
- Automatically handles CDC  
- Syncs incremental updates  
- No custom ingestion code required  

**Spark Declarative Pipelines**
- Streams live orders from Event Hubs  
- Auto checkpointing  
- Dependency graph management  
- Incremental processing  

---

## 🔹 Silver Layer (Cleansed + Modeled)

Data is transformed into a Star Schema.

### Dimension Tables
- dim_customers  
- dim_restaurants  
- dim_menu_items  

### Fact Tables
- fact_orders  
- fact_order_items  
- fact_reviews  

### AI-Powered SQL (Mosaic AI)

For fact_reviews, the pipeline uses:

```sql
AI_QUERY(...)
```

This calls a Mosaic AI model endpoint to:
- Perform sentiment analysis (Positive / Neutral / Negative)

Categorize issues:
- Delivery
- Food Quality
- Pricing
- Portion Size

AI inference is executed directly inside SQL pipelines.

---

## 🔹Gold Layer (Business Aggregations)

Optimized materialized views for analytics.

### Tables created:
- Daily Sales Summary
- Restaurant Review Aggregation
- Customer 360 Profile
- Customer 360 Includes
- Lifetime Spend
- Average Order Value
- Favorite Items
- Order Frequency
- Review Behavior

---

# 🔄 Orchestration

Entire pipeline runs through Databricks Workflows.
![Architecture]

## Single job orchestration:

- Streaming ingestion
- Batch CDC sync
- Silver transformations
- AI enrichment
- Gold aggregations
- Dashboard refresh

Fully automated and production-ready.

---
# 📈 Databricks Dashboards


## 1️⃣ Restaurant Chain Performance Dashboard
![Architecture]

### Filters:

Date Range (start_date, end_date)

### KPIs:

- Total Orders
- Total Revenue
- Active Customers
- Unique Customers
- AOV (Average Order Value)

### Visuals:

- Daily Sales Trend
- Best Selling Items
- Order Volume by Day of Week
- Peak Hour Heatmap (Day × Hour)
- Revenue by Order Type
- Revenue by Food Category

## 2️⃣ Review Insights Dashboard
![Architecture]

### Filter:

- Restaurant Name

### Metrics:

- Review Volume Over Time
- Avg Rating
- City
- Sentiment Counts (Positive / Neutral / Negative)
- Sentiment Trend
- Ratings Distribution
- Issue Categorization:
- Delivery
- Food Quality
- Pricing
- Portion Size

Recent Review Feed

## 🧪 Synthetic Data Generation

- All data is generated using Python.
- Batch Data Generator
- Creates CSV files
- Loads them into Azure SQL

### Tables:

- customers
- restaurants
- historical_orders
- menu_items
- reviews

#### Streaming Data Generator
#### Produces incremental JSON orders
#### Publishes to Azure Event Hubs
#### Simulates live POS activity
#### Dataset modeled for UAE restaurant market.


--- 
# 📁 Repository Structure

```bash
📁 Project folder
├── data/
│   ├── synthetic_data_python_scripts/
│   ├── sql_table_structures/
│   └── event_hub_incremental_scripts/
│
├── databricks_pipeline/
│   ├── ingestion/
│   │   ├── lakeflow_connect/
│   │   └── spark_declarative_pipeline/
│   ├── silver/
│   └── gold/
│
├── databricks_dashboard/
│   ├── dashboard_pdfs/
│   └── dashboard_sql_scripts/
│
├── images/
│   ├── architecture.png
│   └── workflow.png
│
└── README.md
```

---
# 🔐 Governance & Security

### Managed via Unity Catalog

### Role-Based Access Control

### Schema-level isolation:

- landing
- bronze
- silver
- gold

Centralized metadata governance

---
# 🚀 Key Engineering Highlights

- ✔ Real-time + batch hybrid ingestion
- ✔ CDC without custom code
- ✔ SQL-based AI inference
- ✔ Star schema modeling
- ✔ Customer 360 analytics
- ✔ Native BI dashboards
- ✔ End-to-end orchestration
- ✔ Production-grade governance

---
# 🎯 Project Value

## This project demonstrates practical expertise in:

- Modern Lakehouse Architecture
- Databricks Advanced Features
- Streaming Data Engineering
- CDC Patterns
- AI integration in SQL pipelines
- Production orchestration
- Business-focused data modeling

### It reflects real-world data engineering challenges across ingestion, modeling, AI enrichment, governance, and analytics delivery.