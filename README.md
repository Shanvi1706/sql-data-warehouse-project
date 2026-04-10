## 📊 End-to-End Data Warehouse & ETL Pipeline Project
## 📌 Overview

This project demonstrates the design and implementation of an end-to-end data warehouse pipeline for transforming raw data into meaningful business insights.

The pipeline follows a multi-layered architecture (Bronze → Silver → Gold) to ensure data quality, scalability, and efficient analytics.

The final processed data is visualized through an interactive dashboard for decision-making.

---
## Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver** and **Gold** layers:
<img width="1080" height="571" alt="data architecture1 drawio" src="https://github.com/user-attachments/assets/9ddcacae-1687-4e81-a3ec-9398f024db72" />

🔹 Bronze Layer
Raw data ingestion from multiple sources (CRM & ERP datasets)
Data stored in its original format
Minimal transformation
🔹 Silver Layer
Data cleaning and standardization
Handling missing values and inconsistencies
Applying business rules
🔹 Gold Layer
Aggregated and business-ready data
Optimized for analytics and reporting
Used directly for dashboard visualization

---
## 🔄 ETL Pipeline

The pipeline is implemented using Python and SQL:

- Extract: Load raw CSV data from source systems
- Transform: Clean, validate, and structure data
- Load: Store processed data into warehouse tables

Modules:

- extract.py
- transform.py
- load.py
- etl.py (pipeline orchestrator)

---
📊 Data Sources

- CRM Data (Customer, Product, Sales)
- ERP Data (Location, Category, Customer mapping)

---
## 📁 Project Structure

```
sql-data-warehouse-project/
│
├── datasets/               # Raw source data
├── pipeline/               # Python ETL scripts
├── scripts/                # SQL scripts (DDL & procedures)
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│
├── docs/                   # Architecture diagrams & documentation
├── tests/                  # Data quality checks
├── app/                    # Dashboard application
├── README.md
└── .gitignore
```
---
## ⚙️ Tech Stack

- Python (ETL Pipeline)
- SQL (Data Warehouse Design)
- Power BI / Dashboard Tool
- CSV Datasets
- Git & GitHub

---
## 📈 Dashboard

The final data is visualized using an interactive dashboard to provide:

- Sales insights
- Customer analysis
- Product performance metrics

---
## ▶️ How to Run the Project
1. Clone the repository:
git clone https://github.com/Shanvi1706/sql-data-warehouse-project

2. Navigate to project folder:
cd sql-data-warehouse-project

3. Run ETL pipeline:
python pipeline/etl.py

4. Execute SQL scripts to create warehouse tables

5. Connect dashboard to the database

---
## ✅ Key Features
- End-to-end ETL pipeline implementation
- Multi-layered data architecture
- Data cleaning and transformation
- Data quality checks
- Scalable warehouse design
- Dashboard visualization

---
🎯 Conclusion

This project demonstrates how raw data can be transformed into meaningful insights using a structured data engineering pipeline. It highlights practical implementation of ETL processes and data warehousing concepts used in real-world analytics systems.

👩‍💻 Author

Shanvi
B.Tech CSE | Aspiring Data Engineer
