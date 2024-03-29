"# Abigoney_Data-Engineering_Capstone" 

Data Engineering Capstone Project
Project Overview
In this project, I will be working on a data engineering capstone project, where I demonstrate my skills and knowledge gained throughout the Data Engineering Nanodegree program. The goal of this project is to build an end-to-end data pipeline that ingests, processes, and stores large datasets, enabling efficient analysis and querying.

Project Datasets
Earth Surface Temperature Data: Kaggle - https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data
U.S. City Demographic Data: OpenSoft - https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/
Airport Code Table: DataHub.io - https://datahub.io/core/airport-codes#data

Data Model
The purpose of the final data model for this project is to analyze the relationship between airports, temperature, and population to determine if airports have a greater influence on temperatures than population. The data model is designed to support efficient querying and analysis, enabling an examination of airport data conjunction with temperature and demographic data.
By integrating these datasets, we can explore the correlation between airport presence and temperature variations, as well as the influence of population density on temperature changes.  
Furthermore, this data can be valuable for veterans seeking a cool place to live near other veterans with numerous airports. The data model can assist in identifying areas with high concentrations of airports and favorable temperature ranges. The `demo` function in `etl.py` provides example queries that can help such users.

Project Steps
Data Exploration: In this step, I explored the provided datasets to gain a better understanding of the data structure, quality, and potential challenges.

Data Cleaning and Transformation: Based on the insights gained from the exploration step, I cleaned and transform the data to ensure its quality and compatibility with the goals of the final data model.

Data Pipeline Design: To design a scalable and efficient data pipeline, I have utilized AWS EMR with Apache Spark. The pipeline follows a systematic process of ingesting, cleaning, transforming, and loading the data into the final data model. The pipeline also includes writing the results to parquet files for further analysis and storage.
By leveraging the power of AWS EMR and Apache Spark, the data pipeline ensures high scalability, allowing it to handle large volumes of data effectively. The pipeline takes advantage of Spark's distributed computing capabilities, enabling parallel processing and optimizing performance.

Data Quality Checks: In order to ensure the accuracy, completeness, and consistency of the data throughout the pipeline, I have implemented several data quality checks. These checks are crucial for maintaining the integrity of the data and ensuring its suitability for analysis.
Firstly, I have included a data availability check to confirm that the data can be successfully read in from the S3 bucket. This check ensures that the necessary data is accessible and ready for processing.
Secondly, I have implemented a data count check to compare the number of records read from the data source with the number of records written to the destination. This check helps to identify any discrepancies or data loss during the ETL process.
Lastly, I have included a duplicate row check to ensure that there are no duplicate entries in the data. This check helps to maintain data consistency and avoid any potential issues that may arise from duplicate records.
By implementing these data quality checks, I can confidently verify the accuracy, completeness, and consistency of the data at various stages of the pipeline. This ensures that the final data model is reliable and suitable for the intended purpose.

Data Dictionary: I have created a comprehensive data dictionary named Data_Dictionary.md. This document contains detailed information about the final data model, including table schemas, column descriptions, and data types. It serves as a valuable resource for understanding the structure and content of the database.
Additionally, I have included an ER diagram named ER_diagram.pdf. This diagram visually represents the relationships between the tables in an easy-to-understand format. By examining this diagram, you can quickly grasp the connections and dependencies among the tables.
To query the data effectively, you can join the tables based on the city and municipality columns. This will allow you to retrieve all the relevant data related to these entities.

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
