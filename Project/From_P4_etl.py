from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_timestamp
from pyspark.sql.types import TimestampType
from pyspark.sql.types import IntegerType
#import ConfigParser as configparser
import configparser
import psycopg2
print("HI Wourld!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")


"""
    Read in the dwh.cfg information
"""
config = configparser.ConfigParser()
config.read('dwh.cfg')

"""
    Load the AWS keys into the environment
"""
os.environ['KEY'] = config.get('AWS', 'KEY')
os.environ['SECRET'] = config.get('AWS', 'SECRET')

os.environ['HOST'] = config.get('CLUSTER', 'HOST')
os.environ['DB_NAME'] = config.get('CLUSTER', 'DB_NAME')
os.environ['DB_USER'] = config.get('CLUSTER', 'DB_USER')
os.environ['DB_PASSWORD'] = config.get('CLUSTER', 'DB_PASSWORD')
os.environ['DB_PORT'] = config.get('CLUSTER', 'DB_PORT')

"""
    Create the Spark session function
"""
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Creat the process_song_data function
        Input: 
            spark: the spark session
            input_data: the bucket where the input data resides
            output_data: the bucket where to write the resultes to.
        Output:
            songs_table and artists_table are writen to the output buckets after ETL
    """
    """
        Get filepath to song data file
    """
    song_data = os.path.join(input_data, 'song_data/*/*/*/')
    
    """
        Read song data file
    """
    song_df = spark.read.json(song_data)
    
    """
        Extract columns to create songs table
    """
    songs_table =  song_df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates(["song_id"])
    
    """
        Write songs table to parquet files partitioned by year and artist
    """
    songs_table.write.mode("ignore").partitionBy("year", "artist_id").parquet(output_data + "songs/")

    """
        Extract columns to create artists table
    """
    artists_table = song_df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates(["artist_id"])
    
    """
        write artists table to parquet files ignoring anything that already exists
    """
    artists_table.write.mode("ignore").parquet(output_data + "artists/")

"""
    Creating the process_log_data function
        Input: 
            spark: the spark session
            input_data: the bucket where the input data resides
            output_data: the bucket where to write the resultes to.
        Output:
            user_table, time_table, songplays_table are writen to the output_data bucket after ETL
"""
def process_log_data(spark, input_data, output_data):
    """
    Creating the process_log_data function
        Input: 
            spark: the spark session
            input_data: the bucket where the input data resides
            output_data: the bucket where to write the resultes to.
        Output:
            user_table, time_table, songplays_table are writen to the output_data bucket after ETL
    """
    """
        Get filepath to log data file
    """
    log_data = os.path.join(input_data + "log_data/*/*/*")

    """
        Read log data file
    """
    log_df = spark.read.json(log_data, multiLine=True, encoding="utf8")

    """
        Filter by actions for song plays
    """
    log_df.filter(log_df.page == 'NextSong')

    """
        Extract columns for users table    
    """
    user_table = log_df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates(["userID"])

    """
        Write users table to parquet files ignoring anything that already exists
    """
    user_table.write.mode("ignore").parquet(output_data + "user/")

    """
        Create timestamp column from original timestamp column
    """
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))
    
    """
        Create datetime columns from original timestamp column
    """
    get_hour = udf(lambda x: x.hour, IntegerType())
    get_day = udf(lambda x: x.day, IntegerType())
    get_week = udf(lambda x: x.isocalendar()[1], IntegerType())
    get_month = udf(lambda x: x.month, IntegerType())
    get_year = udf(lambda x: x.year, IntegerType())
    get_weekday = udf(lambda x: x.weekday(), IntegerType())

    """
        Apply the UDFs to create new columns and adding them to log_df
    """
    log_df = log_df.withColumn("hour", get_hour(col("timestamp")))
    log_df = log_df.withColumn("day", get_day(col("timestamp")))
    log_df = log_df.withColumn("week", get_week(col("timestamp")))
    log_df = log_df.withColumn("month", get_month(col("timestamp")))
    log_df = log_df.withColumn("year", get_year(col("timestamp")))
    log_df = log_df.withColumn("weekday", get_weekday(col("timestamp")))
    
    
    """
        Extract columns to create time table
    """
    time_table = log_df.select(
        col("timestamp").alias("start_time"),
            "hour",
            "day", 
            "week", 
            "month", 
            "year", 
            "weekday").dropDuplicates(["start_time"])

    """
        Write time table to parquet files partitioned by year and month and ignoring anything that already exists
    """
    time_table.write.mode("ignore").partitionBy("year", "month").parquet(output_data + "logs/")

    """
        Read in song data to use for songplays table
    """
    song_data = os.path.join(input_data, 'song_data/*/*/*/')
    
    """
        Read song data file
    """
    song_df = spark.read.json(song_data)

    """
        Extract the necessary columns from the song data
    """
    song_table = song_df.select("song_id", "title", "artist_id", "duration", "artist_name")

    """
        Joining song and log datasets
    """
    joined_df = log_df.join(song_table, (log_df.song == song_table.title) & (log_df.artist == song_table.artist_name), "inner")
    
    """
        Extract columns from the joined song and log datasets to create songplays table 
    """
    songplays_table = joined_df.select(
        col("timestamp").alias("start_time"),
        col("userId").alias("user_id"),
        "level",
        "song_id",
        "artist_id",
        "year",
        "month",
        col("sessionId").alias("session_id"),
        "location",
        col("userAgent").alias("user_agent")
        ).withColumn("songplay_id", monotonically_increasing_id())

    """
        Write songplays table to parquet files partitioned by year and month
    """
    songplays_table.write.mode("ignore").partitionBy("year", "month").parquet(output_data + "songplays/")


def main():
    """
        Setup the Spark session
    """
    spark = create_spark_session()

    """
        Declaring the input data location
    """
    #input_data = "s3a://udacity-dend/"

    """
        Declaring where to write the output data
    """
    #output_data = "s3a://andrews-udacity/output/"
    
    """
        Calling process_song_data
    """
    #process_song_data(spark, input_data, output_data)    

    """
        Calling process_log_data
    """
    #process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
