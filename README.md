# Data Lake and Data Warehousing Code Examples

This repository contains various Python scripts and Apache Airflow examples used in lectures on Data Lake and Data Warehousing. The purpose of this repository is to provide practical, hands-on code samples that illustrate different techniques and methods for working with data lakes, data warehouses, and cloud-based storage solutions.

## Repository Structure

- **exercises/**: Contains examples and exercises from the lecture series.
  - **data_sample/**: Includes sample data files used in various exercises.
    - `work_status.csv`: A sample CSV file that is used in the data upload and transformation examples.
  - **old_code_samples/**: Contains older versions or reference code from past exercises.
    - `config.yaml`: A configuration file that stores database and AWS credentials.
    - `config_template.yaml`: An anonymized template for configuration settings.

- **Example1_Simple_dag.py**: A basic Apache Airflow DAG example demonstrating simple task scheduling.
- **Example2_load_data_dag.py**: An Airflow DAG that illustrates how to load data into a data lake or warehouse.
- **Example3_Nodes_Sequence.py**: Shows how to use nodes and dependencies in an Airflow DAG.
- **Example4_s3_file_upload_dag.py**: Demonstrates uploading a file to Amazon S3 using an Airflow DAG.
- **sample_boto3_s3.py**: A standalone Python script for interacting with Amazon S3 using the Boto3 library.
- **transfer_CSV_to_RDS.py**: Shows how to transfer data from an S3 bucket to an Amazon RDS instance using Python.

## Setup Instructions

1. **Clone the Repository**
   ```sh
   git clone https://github.com/jose0628/dags.git
   cd dags
