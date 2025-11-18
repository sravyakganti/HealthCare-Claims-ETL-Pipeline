# HealthCare-Claims-ETL-Pipeline
Business Overview
This project implements a robust ETL (Extract, Transform, Load) pipeline for processing healthcare claims data. It is designed to handle high-volume data ingestion, perform data quality checks (removing invalid claim statuses), and load clean data into a data warehouse for downstream analytics.

## Architecture
**Source (CSV) -> Python (Pandas Transformation) -> SQL Database -> Analytics**

## Key Features
*   **Automated Data Ingestion:** Python scripts to read raw claims data.
*   **Data Quality Framework:** Filters out `ERROR` status records and imputes missing diagnosis codes (`NULL` handling).
*   **Logging & Monitoring:** Integrated Python `logging` module to track pipeline health and catch failures.
*   **Analytics Ready:** SQL scripts included to calculate **Denial Rates** and **Cost per Diagnosis**.

## Technologies Used
*   **Python:** Pandas, SQLAlchemy, Faker
*   **SQL:** Advanced aggregation and Window Functions
*   **Workflow:** Modular design compatible with Airflow

## Files Description
*   `etl_pipeline.py`: Main script containing the Extract, Transform, and Load logic.
*   `data_generator.py`: Script to generate realistic mock healthcare data (HIPAA compliant).
*   `analysis_queries.sql`: SQL queries used for reporting on the processed data.
