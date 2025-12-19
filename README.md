# NYC Taxi Big Data Analytics (Spark & Hadoop)

## Project Overview
This project implements an end-to-end **Big Data pipeline** to process and analyze
**NYC Yellow Taxi trip data** at scale using **Apache Spark** and **Hadoop (HDFS)**.

The workflow includes data ingestion, quality checks, cleaning rules,
distributed transformations, and exploratory analysis to extract
interpretable insights from large datasets.

> Note: Code comments and reports may be written in French, as this project was completed in a French academic context.

---

## Dataset
- **Name**: NYC Yellow Taxi Trip Data
- **Source**: NYC TLC / Kaggle
- **Format**: CSV
- **Scale**: ~12+ million records

*(The raw dataset is not included in this repository due to its size.)*

---

## Objectives
- Load large CSV files from **HDFS** into Spark
- Apply **data quality validation** (nulls, invalid values)
- Clean and filter records using business rules
- Perform distributed transformations with Spark DataFrames and RDDs
- Produce aggregated analytics for decision support
- Save cleaned outputs back to HDFS

---

## Pipeline Summary
1. **Ingestion**: CSV → HDFS → Spark
2. **Data Quality**: null checks, invalid ranges
3. **Cleaning**: filtering, deduplication
4. **Transformations**: feature creation, aggregations
5. **Analysis**: temporal and behavioral insights
6. **Output**: cleaned data saved to HDFS

---

## Tech Stack
- **Big Data**: Apache Spark (DataFrames, RDD), Spark SQL
- **Storage**: Hadoop (HDFS)
- **Language**: Python (PySpark)
- **Infrastructure**: Docker, Linux
- **Version Control**: Git/GitHub

---

## Repository Structure

nyc-taxi-bigdata-spark/
├── README.md
├── nyc_taxi_spark_project.py
└── report/
    └── Rapport_FINAL.pdf
---
## Automation

The project includes a shell script to automate pipeline execution:

./run.sh

---

## Key Learnings
- Designing scalable Spark pipelines
- Handling data quality at scale
- Using distributed transformations and aggregations
- Producing interpretable analytics from large datasets


---
## Author

Angèle Blandine Feussi Nguemkam

Junior Data Scientist | Applied Data Science

Ottawa, Canada
