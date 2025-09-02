#  Wuzzuf Jobs Data Pipeline

##  Overview
This project builds a **data pipeline** using **Apache Airflow**, **Selenium**, **Snowflake**, and **dbt** to collect job postings from [Wuzzuf](https://wuzzuf.net/) and prepare them for analysis.

---

##  Pipeline Steps

1. **Fetch Data (Selenium)**
   - Scrapes job postings (title, company, location, announcement time, job type, work mode, salary, skills, description, requirements).
   - Stores raw data in a CSV file: `include/jobs.csv`.

2. **Load Data (Snowflake)**
   - Uploads the CSV to Snowflake stage `JOBS_STG`.
   - Copies the data into the `RAW.JOBS` table.

3. **Transform Data (dbt)**
   - Cleans and models job data inside Snowflake.
   - Generates summary tables for analysis.

---

##  Pipeline Diagram
![Pipeline](images/wazzuf_pipeline.png)
##  Tech Stack
- **Apache Airflow** → Orchestration
- **Selenium** → Web Scraping
- **Pandas** → Data Processing
- **Snowflake** → Data Warehouse
- **dbt** → Transformations & Analytics
---------



