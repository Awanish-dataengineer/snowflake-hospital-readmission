# snowflake-hospital-readmission
Snowflake + DBT pipeline for hospital  readmission — 500K+ patient records, star schema,  clustering keys
# 🏥 Hospital Readmission Analysis — Snowflake Data Platform

## 📌 Project Overview
End-to-end data engineering pipeline for hospital 
readmission analysis built on Snowflake Data Warehouse.
Processes 500K+ patient records with high reliability 
and performance.

## 🏗️ Architecture
Raw Data (CSV/S3) → Snowflake (Bronze) → DBT (Silver) 
→ Star Schema (Gold) → Reporting

## 🛠️ Tech Stack
| Tool | Purpose |
|------|---------|
| Snowflake | Data Warehouse & Query Engine |
| DBT | Data Transformation & Modeling |
| Python | Data Ingestion & Automation |
| SQL | Query Writing & Optimization |
| AWS S3 | Raw Data Storage |
| Git | Version Control |

## 📊 Key Features
- ✅ Processes 500K+ patient records
- ✅ Star schema data model for optimized reporting
- ✅ DBT modular transformation layers
- ✅ Clustering keys for Snowflake performance tuning
- ✅ 10+ weekly business reports supported
- ✅ 99%+ data availability

## 📁 Project Structure

├── models/
│   ├── bronze/        # Raw data layer
│   ├── silver/        # Cleaned & standardized
│   └── gold/          # Star schema — facts & dims
├── pipelines/
│   └── ingestion.py   # Python ingestion scripts
├── sql/
│   └── queries/       # Optimized SQL queries
├── tests/             # DBT data quality tests
└── README.md


## 🔑 Key Achievements
- Reduced query processing time by **60%** using 
  Snowflake clustering keys
- Built modular DBT models enabling faster 
  development and testing
- Designed star schema supporting 10+ business 
  reports per week

## 📬 Contact
**Awanish Kumar** — Senior Data Engineer
- 📧 awshi91@gmail.com
- 🔗 linkedin.com/in/awanish-kumar-4621a1a1
