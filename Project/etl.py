import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, split
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F

"""
    Read in the dwh.cfg information
"""
config = configparser.ConfigParser()
config.read('dwh.cfg')

"""
    Load the AWS keys into the environment
"""
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'SECRET')

AWS_ACCESS_KEY_ID = config.get('AWS', 'KEY')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'SECRET')

"""
    Declaring the input data location
"""
input_bucket = config.get('S3', 'INPUT_BUCKET')

"""
    Declaring the temp bucket that I have Write access to
"""
temp_bucket = config.get('S3', 'TEMP_BUCKET')

print(f"AWS_ACCESS_KEY_ID {AWS_ACCESS_KEY_ID}")
db_iam = config.get('IAM_ROLE', 'ARN')
db_host = config.get('CLUSTER', 'HOST')
# jdbc_host = f"jdbc:redshift://{db_host}:5439/dev"
db_name = config.get('CLUSTER', 'DB_NAME')
jdbc_host = f"jdbc:redshift://{db_host}:5439/{db_name}"
db_user = config.get('CLUSTER', 'DB_USER')
db_password = config.get('CLUSTER', 'DB_PASSWORD')
db_port = config.get('CLUSTER', 'DB_PORT')

"""
    Create the Spark session function
"""
def create_spark_session():
    """
    Create and return a SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

"""
    Read the US Cities Demographics data and clean for joining
"""
def read_cities_demographics(spark, input_bucket, input_file):
    """
    Read the US Cities Demographics data from the specified input file
    """
    input_data = os.path.join(input_bucket, input_file)
    demographics_df = spark.read.option("header", True).json(input_data)

    """
    Rename columns for consistency
    """
    demographics_df = demographics_df.withColumnRenamed("city", "city") \
        .withColumnRenamed("State", "state") \
        .withColumnRenamed("Median Age", "median_age") \
        .withColumnRenamed("Male Population", "male_population") \
        .withColumnRenamed("Female Population", "female_population") \
        .withColumnRenamed("Total Population", "total_population") \
        .withColumnRenamed("number_of_veterans", "veteran_population") \
        .withColumnRenamed("Foreign-born", "foreign_born") \
        .withColumnRenamed("average_household_size", "ave_household_size") \
        .withColumnRenamed("State Code", "state_code") \
        .withColumnRenamed("Race", "race") \
        .withColumnRenamed("Count", "count")

    """
    Cast columns for consistency
    """
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

    """
    QA the Data to make sure we read something in
    """
    if demographics_df.count() == 0:
        print("Error: No data to process.")
        exit()

    """
    Return the Dataframe
    """
    return demographics_df

"""
    Read the airport codes data and clean for joining
"""
def read_airport_codes(spark, input_bucket, input_file):
    """
    Read the airport codes data from the specified input file
    """
    input_data = os.path.join(input_bucket, input_file)
    airport_df = spark.read.option("header", True).csv(input_data)

    """
    Rename the name column for uniqueness
    """
    airport_df = airport_df.withColumnRenamed("name", "airtport_name")

    """
    Limit the data to only US
    """
    airport_df = airport_df.filter(F.col("iso_country") == "US")

    """
    Split coordinates column into lat long
    """
    airport_df = airport_df.withColumn("latitude", split(airport_df["coordinates"], ", ")[1])
    airport_df = airport_df.withColumn("longitude", split(airport_df["coordinates"], ", ")[0])

    """
    Cast the new columns as floats
    """
    airport_df = airport_df.withColumn("latitude", col("latitude").cast("float")) \
        .withColumn("longitude", col("longitude").cast("float"))

    """
    Drop the original coordinates column
    """
    airport_df = airport_df.drop("coordinates")

    """
    Data quality validation
    """
    if airport_df.count() == 0:
        print("Error: No data to process.")
        exit()

    """
    Return the Dataframe
    """
    return airport_df

"""
    Read the Surface Temp data and clean for joining
"""
def read_city_temp_data(spark, input_bucket, input_file):
    """
    Read the Surface Temp data from the specified input file
    """
    input_data = os.path.join(input_bucket, input_file)
    city_ground_temp_df = spark.read.option("header", True).csv(input_data)

    """
    Renaming the columns
    """
    city_ground_temp_df = city_ground_temp_df.withColumnRenamed("dt", "date") \
        .withColumnRenamed("AverageTemperature", "average_temperature") \
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temperature_uncertainty") \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("Country", "country") \
        .withColumnRenamed("Latitude", "latitude") \
        .withColumnRenamed("Longitude", "longitude")

    """
    Limit this to the United States to make it run faster
    """
    city_ground_temp_df = city_ground_temp_df.filter(F.col("country") == "United States")

    """
    Split date column into year, month
    """
    city_ground_temp_df = city_ground_temp_df.withColumn("year", split(city_ground_temp_df["date"], "-")[0])
    city_ground_temp_df = city_ground_temp_df.withColumn("mon", split(city_ground_temp_df["date"], "-")[1])

    """
    Drop the original date column
    """
    city_ground_temp_df = city_ground_temp_df.drop("date")

    """
    Define a function to convert and strip the latitude/longitude values
    """
    def convert_coordinates(coord):
        # Need to return `None` when there is no Lat Long
        if coord is None:
            return None

        value = float(coord[:-1])
        if coord[-1] == 'S' or coord[-1] == 'W':
            value *= -1
        return float(value)

    convert_coordinates_udf = udf(lambda coord: convert_coordinates(coord), FloatType())

    """
    Apply the UDF to the 'latitude' and 'longitude' columns
    """
    city_ground_temp_df = city_ground_temp_df.withColumn('latitude', convert_coordinates_udf('latitude'))
    city_ground_temp_df = city_ground_temp_df.withColumn('longitude', convert_coordinates_udf('longitude'))

    """
    Cast all the columns
    """
    city_ground_temp_df = city_ground_temp_df.withColumn("year", col("year").cast("int")) \
        .withColumn("mon", col("mon").cast("int")) \
        .withColumn("average_temperature", col("average_temperature").cast("float")) \
        .withColumn("average_temperature_uncertainty", col("average_temperature_uncertainty").cast("float")) \
        .withColumn("city", col("city").cast("varchar(256)")) \
        .withColumn("country", col("country").cast("varchar(256)")) \
        .withColumn("latitude", col("latitude").cast("decimal(9, 2)")) \
        .withColumn("longitude", col("longitude").cast("decimal(9, 2)"))

    """
    Clean up the data by dropping rows with null for average_temperature
    """
    city_ground_temp_df = city_ground_temp_df.dropna(subset=["average_temperature"])

    """
    Data quality validation
    """
    if city_ground_temp_df.count() == 0:
        print("Error: No data to process.")
        exit()

    """
    Return the Dataframe
    """
    return city_ground_temp_df

"""
    Create and write the Fact and Dimension tables
"""
def make_tables(temp_bucket, airport_df, city_ground_temp_df, demographics_df):
    """
    Join city_ground_temp_df, airport_df, and demographics_df to create the Fact table
    """
    fact_table = city_ground_temp_df.alias("gtemp").join(
        airport_df.alias("air"),
        city_ground_temp_df.city == airport_df.municipality,
        "inner"
    ).join(
        demographics_df.alias("demog"),
        city_ground_temp_df.city == demographics_df.city,
        "inner"
    ).select(
        "gtemp.average_temperature",
        "gtemp.average_temperature_uncertainty",
        "gtemp.city",
        "gtemp.country",
        "gtemp.latitude",
        "gtemp.longitude",
        "gtemp.year",
        "gtemp.mon",
        "air.ident",
        "air.type",
        "air.airtport_name",
        "air.elevation_ft",
        "air.continent",
        "air.iso_country",
        "air.iso_region",
        "air.municipality",
        "air.gps_code",
        "air.iata_code",
        "air.local_code",
        "demog.ave_household_size",
        "demog.count",
        "demog.female_population",
        "demog.foreign_born",
        "demog.male_population",
        "demog.median_age",
        "demog.veteran_population",
        "demog.race",
        "demog.state",
        "demog.state_code",
        "demog.total_population"
    )

    """
    Create dimension tables
    """
    dimension_table_airport = airport_df.select("municipality", "elevation_ft", "airtport_name", "iso_country", "iso_region")
    dimension_table_demographics = demographics_df.select("city", "state", "median_age", "male_population", "female_population", "total_population", "veteran_population", "foreign_born", "ave_household_size", "state_code", "race", "count")

    """
    Write tables to Parquet files in the temp_bucket
    """
    output_folder = os.path.join(temp_bucket, "capstone_output")
    fact_table.write.mode("overwrite").parquet(os.path.join(output_folder, "fact_table"))
    dimension_table_airport.write.mode("overwrite").parquet(os.path.join(output_folder, "dimension_table_airport"))
    dimension_table_demographics.write.mode("overwrite").parquet(os.path.join(output_folder, "dimension_table_demographics"))

"""
    Run QA on the ingested data
"""
def run_qa(spark):
    """
    First QA we need to confirm the data can be read in
    """
    input_folder = os.path.join(temp_bucket, "capstone_output")
    qa_fact_table = spark.read.parquet(os.path.join(input_folder, "fact_table"))
    qa_dimension_table_airport = spark.read.parquet(os.path.join(input_folder, "dimension_table_airport"))
    dimension_table_demographics = spark.read.parquet(os.path.join(input_folder, "dimension_table_demographics"))

    """
    If we are successful at reading in the data, let's run a query on it to make sure it can be used for what we want it for
    """
    """
    Calculate the average temperature by city
    """
    average_temperature_by_city = qa_fact_table.groupBy("city").agg(avg("average_temperature").alias("avg_temperature"))

    """
    Join the average temperature with the airport elevation and city population
    """
    result = average_temperature_by_city.join(qa_dimension_table_airport, average_temperature_by_city.city == qa_dimension_table_airport.municipality, "inner") \
        .join(dimension_table_demographics, average_temperature_by_city.city == dimension_table_demographics.city, "inner")

    """
    Calculate the average temperature difference based on airport elevation and population
    """
    result = result.withColumn("temperature_difference", result.avg_temperature - result.elevation_ft / 1000 - result.total_population / 1000000)

    """
    Show the second QA result
    """
    print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<QA Results>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    result.show()

    print(f"*************************************************************qa_fact_table**********************************************************")
    qa_fact_table.show()
    print(f"*************************************************************qa_dimension_table_airport**********************************************************")
    qa_dimension_table_airport.show()
    print(f"*************************************************************dimension_table_demographics**********************************************************")
    dimension_table_demographics.show()

def main():
    """
    Setup the Spark session
    """
    spark = create_spark_session()

    """
    Read in the input data
    """
    airport_df = read_airport_codes(spark, input_bucket, "airport-codes_csv.csv")
    city_ground_temp_df = read_city_temp_data(spark, input_bucket, "Surface_Temps/GlobalLandTemperaturesByMajorCity.csv")
    demographics_df = read_cities_demographics(spark, input_bucket, "us-cities-demographics.json")

    """
    Create the tables and write out the parquet files
    """
    make_tables(temp_bucket, airport_df, city_ground_temp_df, demographics_df)

    """
    Run QA to make sure it all worked
    """
    run_qa(spark)


if __name__ == "__main__":
    main()