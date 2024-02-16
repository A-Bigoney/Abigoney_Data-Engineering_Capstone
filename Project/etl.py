from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp
from pyspark.sql.types import TimestampType
from pyspark.sql.types import IntegerType
import configparser
#import psycopg2
from pyspark.sql import SQLContext
from pyspark.sql.functions import split
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when 

import sys

print("Pthon Verstion!!!!!!!!!!!!!!!!!!!!!!!!!")
print(sys.version)

"""
    Read in the dwh.cfg information
"""
config = configparser.ConfigParser()
config.read('dwh.cfg')

"""
    Load the AWS keys into the environment
"""
os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'SECRET')

AWS_ACCESS_KEY_ID=config.get('AWS', 'KEY')
AWS_SECRET_ACCESS_KEY=config.get('AWS', 'SECRET')

print(f"AWS_ACCESS_KEY_ID {AWS_ACCESS_KEY_ID}")
db_iam = config.get('IAM_ROLE', 'ARN')
db_host = config.get('CLUSTER', 'HOST')
#jdbc_host = f"jdbc:redshift://{db_host}:5439/dev"
db_name = config.get('CLUSTER', 'DB_NAME')
jdbc_host = f"jdbc:redshift://{db_host}:5439/{db_name}"
db_user = config.get('CLUSTER', 'DB_USER')
db_password = config.get('CLUSTER', 'DB_PASSWORD')
db_port = config.get('CLUSTER', 'DB_PORT')

"""
    Create the Spark session function
"""
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

#def create_spark_session():
#    spark = SparkSession \
#        .builder \
#        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
#        .config("spark.hadoop.fs.s3a.region", "us-west-2") \
#        .getOrCreate()
#    return spark


def upload_imigration_data(spark, input_bucket, temp_bucket, input_file, output_table):
    """
        Get csv filepath to read in
    """
    input_data = os.path.join(input_bucket, input_file)
    print(f"Reading from {input_data} ===============================================================================================")

    """
        Reading csv in
    """
    immigration_df = spark.read.option("header", True).csv(input_data)
    #csv_df.head()
    immigration_df.show()
    # rename columns
    #data = immigration_df.withColumnRenamed("_c0", "c0") \
    #    .withColumnRenamed("cicid", "order_id") \
    #    .withColumnRenamed("", "") \
    #    .withColumnRenamed("Order Amount", "order_amount") 

    # convert data types
    immigration_df = immigration_df.withColumn("_c0", col("_c0").cast("int")) \
        .withColumn("cicid", col("cicid").cast("int")) \
        .withColumn("i94yr", col("i94yr").cast("int")) \
        .withColumn("i94mon", col("i94mon").cast("int")) \
        .withColumn("i94cit", col("i94cit").cast("int")) \
        .withColumn("i94res", col("i94res").cast("int")) \
        .withColumn("i94port", col("i94port").cast("varchar(256)")) \
        .withColumn("arrdate", col("arrdate").cast("int")) \
        .withColumn("i94mode", col("i94mode").cast("int")) \
        .withColumn("i94addr", col("i94addr").cast("varchar(256)")) \
        .withColumn("depdate", col("depdate").cast("int")) \
        .withColumn("i94bir", col("i94bir").cast("int")) \
        .withColumn("i94visa", col("i94visa").cast("int")) \
        .withColumn("count", col("count").cast("int")) \
        .withColumn("dtadfile", col("dtadfile").cast("int")) \
        .withColumn("visapost", col("visapost").cast("varchar(256)")) \
        .withColumn("occup", col("occup").cast("varchar(256)")) \
        .withColumn("entdepa", col("entdepa").cast("varchar(256)")) \
        .withColumn("entdepd", col("entdepd").cast("varchar(256)")) \
        .withColumn("entdepu", col("entdepu").cast("varchar(256)")) \
        .withColumn("matflag", col("matflag").cast("varchar(256)")) \
        .withColumn("biryear", col("biryear").cast("int")) \
        .withColumn("dtaddto", col("dtaddto").cast("int")) \
        .withColumn("gender", col("gender").cast("varchar(256)")) \
        .withColumn("insnum", col("insnum").cast("varchar(256)")) \
        .withColumn("airline", col("airline").cast("varchar(256)")) \
        .withColumn("admnum", col("admnum").cast("int")) \
        .withColumn("fltno", col("fltno").cast("varchar(256)")) \
        .withColumn("visatype", col("visatype").cast("varchar(256)")) 
    
    print("*********************************Showing changed data types befor posting immigration data to redshift************************************")

    # Drop the _c0 column
    immigration_df = immigration_df.drop("_c0")

    immigration_df.show()
    # data quality validation
    if immigration_df.count() == 0:    
        print("Error: No data to process.")
        exit() 

    # load data into Redshift
    immigration_df.write \
        .format("jdbc") \
        .option("url", jdbc_host) \
        .option("dbtable", output_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("aws_iam_role", db_iam) \
        .option("tempdir", os.path.join(temp_bucket,"temp/")) \
        .mode("overwrite") \
        .save() 
    
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Ingested {output_table} data into Redshift!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")



def upload_cities_demographics(spark, input_bucket, temp_bucket, input_file, output_table):
    """
        Get csv filepath to read in
    """
    input_data = os.path.join(input_bucket, input_file)
    print(f"Reading from {input_data} ===============================================================================================")

    """
        Reading csv in
    """
    demographics_df = spark.read.option("header", True).option("delimiter", ";").csv(input_data)
    #csv_df.head()
    demographics_df.show()

    if demographics_df.count() == 0:    
        print("Error: No data to process.")
        exit() 


    # rename columns
    demographics_df = demographics_df.withColumnRenamed("City", "city") \
        .withColumnRenamed("State", "state") \
        .withColumnRenamed("Median Age", "median_age") \
        .withColumnRenamed("Male Population", "male_population") \
        .withColumnRenamed("Female Population", "female_population") \
        .withColumnRenamed("Total Population", "total_population") \
        .withColumnRenamed("Number of Veterans", "veteran_population") \
        .withColumnRenamed("Foreign-born", "foreign_born") \
        .withColumnRenamed("Average Household Size", "ave_household_size") \
        .withColumnRenamed("State Code", "state_code") \
        .withColumnRenamed("Race", "race") \
        .withColumnRenamed("Count", "count") 


    # convert data types
    demographics_df = demographics_df.withColumn("city", col("city").cast("varchar(256)")) \
        .withColumn("state", col("state").cast("varchar(256)")) \
        .withColumn("median_age", col("median_age").cast("float")) \
        .withColumn("male_population", col("male_population").cast("int")) \
        .withColumn("female_population", col("female_population").cast("int")) \
        .withColumn("total_population", col("total_population").cast("int")) \
        .withColumn("veteran_population", col("veteran_population").cast("int")) \
        .withColumn("foreign_born", col("foreign_born").cast("int")) \
        .withColumn("ave_household_size", col("ave_household_size").cast("float")) \
        .withColumn("state_code", col("state_code").cast("varchar(2)")) \
        .withColumn("race", col("race").cast("varchar(256)")) \
		.withColumn("count", col("count").cast("int"))
        

    
    print(f"*********************************Showing changed data types befor posting {output_table} data to redshift************************************")

    demographics_df.show()
    # data quality validation
    if demographics_df.count() == 0:    
        print("Error: No data to process.")
        exit() 

    # load data into Redshift
    demographics_df.write \
        .format("jdbc") \
        .option("url", jdbc_host) \
        .option("dbtable", output_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("aws_iam_role", db_iam) \
        .option("tempdir", os.path.join(temp_bucket,"temp/")) \
        .mode("overwrite") \
        .save() 
    
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Ingested {output_table} data into Redshift!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")




def upload_airport_codes(spark, input_bucket, temp_bucket, input_file, output_table):
    """
        Get csv filepath to read in
    """
    input_data = os.path.join(input_bucket, input_file)
    print(f"Reading from {input_data} ===============================================================================================")

    """
        Reading csv in
    """
    airport_df = spark.read.option("header", True).csv(input_data)
    #csv_df.head()
    airport_df.show()

    if airport_df.count() == 0:    
        print("Error: No data to process.")
        exit() 

    #Splite coordinates column into lat long
    airport_df = airport_df.withColumn("latitude", split(airport_df["coordinates"], ", ")[0])
    airport_df = airport_df.withColumn("longitude", split(airport_df["coordinates"], ", ")[1])

    # Drop the original coordinates column
    airport_df = airport_df.drop("coordinates")
    

    # convert data types
    #airport_df = airport_df.withColumn("city", col("city").cast("varchar(256)")) \
    #    .withColumn("state", col("state").cast("varchar(256)")) \
    #    .withColumn("median_age", col("median_age").cast("float")) \
        

    
    print(f"*********************************Showing changed data types befor posting {output_table} data to redshift************************************")

    airport_df.show()
    # data quality validation
    if airport_df.count() == 0:    
        print("Error: No data to process.")
        exit() 

    # load data into Redshift
    airport_df.write \
        .format("jdbc") \
        .option("url", jdbc_host) \
        .option("dbtable", output_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("aws_iam_role", db_iam) \
        .option("tempdir", os.path.join(temp_bucket,"temp/")) \
        .mode("overwrite") \
        .save() 
    
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Ingested {output_table} data into Redshift!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")



def upload_imigration_parquet_data(spark, input_bucket, temp_bucket, input_file, output_table):
    """
        Get csv filepath to read in
    """
    input_data = os.path.join(input_bucket, input_file)
    print(f"Reading from {input_data} ===============================================================================================")

    """
        Reading csv in
    """
    immigration_df = spark.read.parquet(input_data)
    #csv_df.head()
    print(f"*********************************Showing UNchanged data types from {output_table} data to redshift************************************")
    immigration_df.show()
    
    immigration_df = immigration_df.withColumn("cicid", col("cicid").cast("int")) \
        .withColumn("i94yr", col("i94yr").cast("int")) \
        .withColumn("i94mon", col("i94mon").cast("int")) \
        .withColumn("i94cit", col("i94cit").cast("int")) \
        .withColumn("i94res", col("i94res").cast("int")) \
        .withColumn("i94port", col("i94port").cast("varchar(256)")) \
        .withColumn("arrdate", col("arrdate").cast("int")) \
        .withColumn("i94mode", col("i94mode").cast("int")) \
        .withColumn("i94addr", col("i94addr").cast("varchar(256)")) \
        .withColumn("depdate", col("depdate").cast("int")) \
        .withColumn("i94bir", col("i94bir").cast("int")) \
        .withColumn("i94visa", col("i94visa").cast("int")) \
        .withColumn("count", col("count").cast("int")) \
        .withColumn("dtadfile", col("dtadfile").cast("int")) \
        .withColumn("visapost", col("visapost").cast("varchar(256)")) \
        .withColumn("occup", col("occup").cast("varchar(256)")) \
        .withColumn("entdepa", col("entdepa").cast("varchar(256)")) \
        .withColumn("entdepd", col("entdepd").cast("varchar(256)")) \
        .withColumn("entdepu", col("entdepu").cast("varchar(256)")) \
        .withColumn("matflag", col("matflag").cast("varchar(256)")) \
        .withColumn("biryear", col("biryear").cast("int")) \
        .withColumn("dtaddto", col("dtaddto").cast("int")) \
        .withColumn("gender", col("gender").cast("varchar(256)")) \
        .withColumn("insnum", col("insnum").cast("varchar(256)")) \
        .withColumn("airline", col("airline").cast("varchar(256)")) \
        .withColumn("admnum", col("admnum").cast("int")) \
        .withColumn("fltno", col("fltno").cast("varchar(256)")) \
        .withColumn("visatype", col("visatype").cast("varchar(256)")) 

    print(f"*********************************Showing changed data types befor posting {output_table} data to redshift************************************")

    immigration_df.show()
    # data quality validation
    if immigration_df.count() == 0:    
        print("Error: No data to process.")
        exit() 



    # load data into Redshift
    immigration_df.write \
        .format("jdbc") \
        .option("url", jdbc_host) \
        .option("dbtable", output_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("aws_iam_role", db_iam) \
        .option("tempdir", os.path.join(temp_bucket,"temp/")) \
        .mode("overwrite") \
        .save() 
    
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Ingested {output_table} data into Redshift!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")



def upload_temp_data(spark, input_bucket, temp_bucket, input_file, output_table):
    """
        Get csv filepath to read in
    """
    input_data = os.path.join(input_bucket, input_file)
    print(f"Reading from {input_data} ===============================================================================================")

    """
        Reading csv in
    """
    ground_temp_df = spark.read.option("header", True).csv(input_data)
    #csv_df.head()
    print(f"*********************************Showing UNchanged data types from {output_table} data to redshift************************************")
    ground_temp_df.show()

    ground_temp_df = ground_temp_df.withColumnRenamed("dt", "date") \
        .withColumnRenamed("AverageTemperature", "average_temperature") \
		.withColumnRenamed("AverageTemperatureUncertainty", "average_temperature_uncertainty") \
		.withColumnRenamed("City", "city") \
		.withColumnRenamed("Country", "country") \
		.withColumnRenamed("Latitude", "latitude") \
		.withColumnRenamed("Longitude", "longitude") 

    print(f"*********************************Showing changed Coloum names from {output_table} data to redshift************************************")
    ground_temp_df.show()

    #Splite date column into year, mon
    ground_temp_df = ground_temp_df.withColumn("year", split(ground_temp_df["date"], "-")[0])
    ground_temp_df = ground_temp_df.withColumn("mon", split(ground_temp_df["date"], "-")[1])

    # Drop the original date column
    ground_temp_df = ground_temp_df.drop("date")

    ground_temp_df = ground_temp_df.withColumn("year", col("year").cast("int")) \
            .withColumn("mon", col("mon").cast("int")) \
            .withColumn("average_temperature", col("average_temperature").cast("float")) \
            .withColumn("average_temperature_uncertainty", col("average_temperature_uncertainty").cast("float")) \
            .withColumn("city", col("city").cast("varchar(256)")) \
            .withColumn("country", col("country").cast("varchar(256)")) \
            .withColumn("latitude", col("latitude").cast("float")) \
            .withColumn("longitude", col("longitude").cast("float"))

    
    print(f"*********************************Showing changed data types befor posting {output_table} data to redshift************************************")

    ground_temp_df.show()
    # data quality validation
    if ground_temp_df.count() == 0:    
        print("Error: No data to process.")
        exit() 



    # load data into Redshift
    ground_temp_df.write \
        .format("jdbc") \
        .option("url", jdbc_host) \
        .option("dbtable", output_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("aws_iam_role", db_iam) \
        .option("tempdir", os.path.join(temp_bucket,"temp/")) \
        .mode("overwrite") \
        .save() 
    
    print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Ingested {output_table} data into Redshift!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

def creat_fact_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS fact_table AS (
            SELECT
                i.cicid,
                i.i94yr,
                i.i94mon,
                i.i94cit,
                i.i94res,
                i.i94port,
                i.arrdate,
                i.i94mode,
                i.i94addr,
                i.depdate,
                i.i94bir,
                i.i94visa,
                i.count,
                i.dtadfile,
                i.visapost,
                i.occup,
                i.entdepa,
                i.entdepd,
                i.entdepu,
                i.matflag,
                i.biryear,
                i.dtaddto,
                i.gender,
                i.insnum,
                i.airline,
                i.admnum,
                i.fltno,
                i.visatype,
                s.average_temperature,
                s.average_temperature_uncertainty,
                d.median_age,
                d.male_population,
                d.female_population,
                d.total_population,
                d.veteran_population,
                d.foreign_born,
                d.ave_household_size,
                d.state_code,
                d.race,
                d.count
            FROM immigration i
            JOIN surface_temps s ON i.i94yr = s.year AND i.i94mon = s.mon AND i.i94port = s.city
            JOIN cities_demog d ON i.i94port = d.city
        )
    """)


def main():
    """
        Setup the Spark session
    """
    spark = create_spark_session()
    """
        Declaring the input data location
    """
    input_bucket = "s3://andrew-capstone-data/Data_Files/"
    """
        Declaring the temp bucket that I have Write access to
    """
    temp_bucket = "s3a://andrews-logging/"
    """
        Running immigration_data_sample.csv
    """

    #upload_cities_demographics(spark, input_bucket, temp_bucket,"us-cities-demographics.csv", "cities_demog" )

    #upload_airport_codes(spark, input_bucket, temp_bucket, "airport-codes_csv.csv", "airport_codes" )

    """
    Select only one Imigration data to upload per run
    the Parquet data takes a long time to run
    """
    #upload_imigration_data(spark, input_bucket, temp_bucket, "immigration_data_sample.csv", "immigration" )
    #upload_imigration_parquet_data(spark, input_bucket, temp_bucket, "sas_data", "immigration" )

    #upload_temp_data(spark, input_bucket, temp_bucket, "Surface_Temps/GlobalLandTemperaturesByMajorCity.csv", "surface_temps" )

    creat_fact_table(spark)


if __name__ == "__main__":
    main()