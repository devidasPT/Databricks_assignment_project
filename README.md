🛒 SuperStore Sales & Business Performance Analytics System

An End-to-End Retail Data Analytics Project using Python, Pandas, PySpark, Databricks, Apache Airflow, and Power BI.

📌 Project Overview

The SuperStore Sales & Business Performance Analytics System is a complete retail analytics solution designed to automate sales data processing, perform profitability analysis, and generate business dashboards for decision-making.

This project simulates a real-world retail environment where sales data is stored in raw CSV files and requires cleaning, transformation, analytics, automation, and visualization.

🏢 Business Problem

A large retail SuperStore operates across multiple regions and product categories such as:

Furniture

Office Supplies

Technology

Current challenges include:

Sales data stored in multiple raw CSV files

Manual and error-prone reporting

Limited visibility into profit and discount patterns

Difficulty identifying loss-making products and regions

The goal is to build a centralized analytics system that automates ETL and provides business insights.

🎯 Project Objectives

Automate sales data ingestion and preprocessing using Python

Perform sales and profitability analysis using Pandas and PySpark

Build scalable analytics using Databricks

Orchestrate workflows using Apache Airflow

Create interactive dashboards using Power BI

Demonstrate real-world retail analytics use cases

🛠️ Technology Stack
🔹 Data Processing

Python

🔹 Big Data & Distributed Analytics

Databricks
PySpark

🔹 Workflow Orchestration

Apache Airflow

🔹 Data Storage

CSV Files

Parquet Files

🔹 Visualization

Power BI

🔹 Version Control & Development

Git & GitHub

Jupyter Notebook / Databricks Notebooks

📂 Project Architecture
Raw CSV Files
        ↓
Python ETL Scripts
        ↓
Cleaned Data (CSV / Parquet)
        ↓
PySpark Analytics (Databricks)
        ↓
Airflow Scheduled Pipelines
        ↓
Power BI Dashboards
📊 Data Scope

The dataset includes:

Order-level sales transactions

Product categories and sub-categories

Customer and regional information

Sales, profit, discount, and quantity

🔹 Core Modules
A. Data Ingestion

Load CSV files using Python

Validate schema and data types

Check date formats and numeric fields

B. Data Cleaning & Transformation

Handle missing and duplicate records

Standardize region and category values

Convert date formats

Create derived metrics:

Total Sales

Profit Margin

Discount Impact

C. Sales & Profitability Analytics

Monthly and yearly sales trends

Category and sub-category performance

Loss-making products identification

Regional performance comparison

Top and bottom performing products

D. Workflow Automation

Build Airflow DAG for ETL

Configure task dependencies

Implement retries and logging

Schedule daily/weekly pipelines

E. Visualization & Reporting

Power BI dashboards include:

Total Sales KPI

Total Profit KPI

Profit Margin %

Category-wise Sales

Region-wise Performance

Monthly Sales Trend

Top 10 Products

🧠 Analytics Techniques Used
Pandas

Filtering and conditional selection

GroupBy and aggregation

Sorting and ranking

Data merging and joins

PySpark

DataFrame transformations

groupBy() operations

Joins between datasets

Aggregation and window functions

⚙️ ETL Workflow

Extract raw CSV data

Validate schema and data consistency

Clean and transform data

Store processed data in Parquet format

Perform analytics using PySpark

Load final dataset into Power BI

Automate pipeline using Airflow

✅ Testing & Validation

Data quality checks after ETL

Validation of sales and profit calculations

Duplicate record verification

Dashboard KPI validation

📦 Deliverables

Python ETL Scripts

Databricks Analytics Notebooks

Airflow DAG File

Power BI Dashboard (.pbix)

GitHub Repository

Load processed dataset into Power BI and refresh visuals.

📊 Sample Business Insights

Identify top-performing categories

Detect high-discount, low-profit products

Compare regional profitability

Track monthly sales growth

Find loss-making sub-categories

🏆 Key Learning Outcomes

End-to-end Data Engineering workflow

Real-world ETL pipeline design

Big Data processing using Spark

Workflow automation using Airflow

Business KPI dashboard creation

GitHub project documentation

📌 Future Enhancements

Real-time streaming using Spark Structured Streaming

Cloud storage integration (Azure/AWS)

Advanced forecasting models

Role-based dashboard access

👨‍💻 Devidas
Data Analytics Capstone Project
