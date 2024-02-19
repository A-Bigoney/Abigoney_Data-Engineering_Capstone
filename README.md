"# Abigoney_Data-Engineering_Capstone" 

Data Engineering Capstone Project
Project Overview
In this project, I will be working on a data engineering capstone project, where I demonstrate my skills and knowledge gained throughout the Data Engineering Nanodegree program. The goal of this project is to build an end-to-end data pipeline that ingests, processes, and stores large datasets, enabling efficient analysis and querying.

Project Datasets
Earth Surface Temperature Data: Kaggle - https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data
U.S. City Demographic Data: OpenSoft - https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/
Airport Code Table: DataHub.io - https://datahub.io/core/airport-codes#data

Data Model
The purpose of the final data model for this project is to analyze the relationship between airports, temperature, and population to determine if airports have a greater influence on temperatures than population. The data model is designed to support efficient querying and analysis, enabling a comprehensive examination of airport data conjunction with temperature and demographic data.
The data model is designed to support efficient querying and analysis, enabling a comprehensive examination of temperature data in relation to airport locations and population density. By integrating these datasets, we can explore the correlation between airport presence and temperature variations, as well as the influence of population density on temperature changes.

Project Steps
Data Exploration: In this step, I explored the provided datasets to gain a better understanding of the data structure, quality, and potential challenges.

Data Cleaning and Transformation: Based on the insights gained from the exploration step, I cleaned and transform the data to ensure its quality and compatibility with the final data model.

Data Pipeline Design: To design a scalable and efficient data pipeline, I have utilized AWS EMR with Apache Spark. The pipeline follows a systematic process of ingesting, cleaning, transforming, and loading the data into the final data model. The pipeline also includes writing the results to parquet files for further analysis and storage.
By leveraging the power of AWS EMR and Apache Spark, the data pipeline ensures high scalability, allowing it to handle large volumes of data effectively. The pipeline takes advantage of Spark's distributed computing capabilities, enabling parallel processing and optimizing performance.

Data Quality Checks: Throughout the pipeline, I have implemented data quality checks to ensure accurate, complete, and consistent data. Additionally, a QA check function has been included to verify data readability and suitability for running queries that align with the goals of the data model.

Data Dictionary: I will have included `Data_Dictionary.md` as a data dictionary that provides detailed information about the final data model, including table schemas, column descriptions, and data types.

Scaling for Increased Data: The pipeline is designed to easily scale to handle a 100x increase in data volume by simply increasing the number of nodes in the EMR cluster. This will allow the pipeline to handle larger amounts of data efficiently

Pipeline Automation: To automate the ETL pipeline on a daily basis at a 7 am, you can utilize a tool like Airflow. Airflow provides a robust framework for scheduling and orchestrating workflows, making it an ideal choice for automating your ETL process. By configuring Airflow with the desired schedule and dependencies, you can ensure that your pipeline runs automatically and reliably every day.

Access for Multiple Users: To enable concurrent access by 100+ users, the database can be configured accordingly. The data is stored in Parquet files on an S3 bucket, facilitating easy sharing with a large number of people simultaneously. However, if there are limitations on iterative usage by the users, an alternative option would be to write the data to a Redshift database. This would also allow for scaling the cluster size based on demand.

Technologies Used:
Python
Apache Spark
AWS S3



Instructions to Run the Project
1.  Clone the project repository by running the following command in your terminal:
    `git clone https://github.com/A-Bigoney/Abigoney_Data-Engineering_Capstone.git`

2.  Fill in the configuration file:
    Open the file `Project\Sample-dwh.cfg` and fill in the necessary details. Save the file as `Project\dwh.cfg`.

3.  Upload the `etl.py` script and `dwh.cfg` configuration file to an Amazon EMR cluster. Make sure the cluster is running with the release label emr-6.15.0 and has the Spark and Zeppelin applications installed.

4.  SSH into the EMR cluster using the appropriate command.

5.  Run the ETL script by executing the following command:
    `spark-submit --master yarn ./etl.py`
