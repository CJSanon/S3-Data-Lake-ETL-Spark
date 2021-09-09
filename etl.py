import configparser
import logging
from datetime import datetime
import os
import boto3
import timeit
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_bucket():
    """
    Creates a bucket for output data
    
    Parameters:
    N/A
    
    Returns:
    AWS S3 bucket created with user-defined bucket name and returns bucket_name 
    
    """
    
    bucket_name = 'sparkify-analytics-tables'

    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=bucket_name)
    
    return bucket_name


def create_spark_session():
    """
    Returns a Spark Session with Hadoop integration.
    
    Parameters:
    N/A
    
    Returns:
    A Spark session
    """
    
    # creates a Spark session
    spark = SparkSession.builder\
        .appName("fullDataset")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .config("spark.dynamicAllocation.enabled", "true")\
        .config("spark.hadoop.parquet.enable.summary-metadata.level", "false")\
        .config("spark.sql.parquet.mergeSchema", "false")\
        .config("spark.sql.parquet.filterPushdown", "true")\
        .config("spark.sql.hive.metastorePartitionPruning", "true")\
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")\
        .getOrCreate()
    
    spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark
    
    
    
def process_song_data(spark, input_data, output_data):
    """
    Loads in song data, extracts and transforms into songs and artists dimensional tables, and finally loads the dimensional tables into an S3 bucket.
    
    Parameters:
    spark         - Spark session that was returned from create_spark_session function.
    input_data    - Path to songs data logs.
    output_data   - Path to S3 bucket to load transformed tables. 
    
    Returns:
    songs_table    - Songs dimensional table returned as Spark DataFrame
    artists_table  - Artists dimensional table returned as Spark DataFrame
    """
    
    song_data_schema = StructType(\
                                     [StructField("artist_id", StringType(), True), \
                                      StructField("artist_latitude", DoubleType(), True), \
                                      StructField("artist_location", StringType(), True), \
                                      StructField("artist_longitude", DoubleType(), True), \
                                      StructField("artist_name", StringType(), True), \
                                      StructField("duration", DoubleType(), True), \
                                      StructField("num_songs", LongType(), True), \
                                      StructField("song_id", StringType(), True), \
                                      StructField("title", StringType(), True), \
                                      StructField("year", LongType(), True)]
                                    )
    # get filepath to song data file
    song_data = "{}song_data/*/*/*/*.json".format(input_data)
    
    # read in songs data and write to a Spark DataFrame
    songs_df = spark.read.json(song_data, song_data_schema)
    print("Loaded song data from filepath {} to Spark DataFrame".format(song_data))

    # extract columns to create songs table and write songs table to parquet files partitioned by year and artist
    songs_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = songs_df.select([col for col in songs_columns])
    songs_table.write.mode("ignore").option("compression", "gzip").partitionBy("year", "artist_id").parquet("{}songs_analytics_tables".format(output_data))
    print('SONGS ANALYTICS TABLE WRITE COMPLETE...')
    

    # extract columns to create artists table and write the artists table to parquet files
    artists_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = songs_df.select([col for col in artists_columns])
    artists_table.write.mode("ignore").option("compression", "gzip").parquet("{}artists_analytics_tables".format(output_data))
    print('ARTISTS ANALYTICS TABLE WRITE COMPLETE...')
    
    return songs_df, songs_table, artists_table
    

def process_log_data(spark, input_data, output_data, songs_df):
    """
    Loads in log data, extracts and transforms log data into users and time dimensional tables and songplays fact table and finally 
    loads the dimensional tables into an S3 bucket then returns users, time, and songplays dataframes
    
    Parameters:
    spark        - Spark session that was returned from create_spark_session function.
    input_data   - Path to songs data logs.
    output_data  - Path to S3 bucket to load transformed tables. 
    
    Returns:
    users_table        - Users dimensional table returned as Spark DataFrame
    time_table         - Time dimensional table returned as Spark DataFrame
    songplays_table    - Songplays fact table returned as Spark DataFrame
    """
    
    # get filepath to log data file
    log_data = "{}log_data/*/*/*.json".format(input_data)
    
    log_data_schema = StructType(\
                        [StructField("artist", StringType(), True),
                         StructField("auth", StringType(), True),
                         StructField("firstName", StringType(), True),
                         StructField("gender", StringType(), True),
                         StructField("itemInSession", LongType(), True),
                         StructField("lastName", StringType(), True),
                         StructField("length", DoubleType(), True),
                         StructField("level", StringType(), True),
                         StructField("location", StringType(), True),
                         StructField("method", StringType(), True),
                         StructField("page", StringType(), True),
                         StructField("registration", DoubleType(), True),
                         StructField("sessionId", LongType(), True),
                         StructField("song", StringType(), True),
                         StructField("status", IntegerType(), True),
                         StructField("ts", LongType(), True),
                         StructField("userAgent", StringType(), True),
                         StructField("userId", StringType(), True)]
                        )
    
    # read log data file
    log_df = spark.read.json(log_data, log_data_schema)
    print("Loaded log_data from filepath {} to Spark DataFrame".format(log_data))
    
    # filter by actions for song plays
    filtered_log_df = log_df.filter(log_df.page == 'NextSong')
    
    # extract columns for users table    
    users_table_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = filtered_log_df.select([col for col in users_table_columns])
    
    # write users table to parquet files
    users_table.write.mode("ignore").option("compression", "gzip").parquet("{}users_analytics_tables".format(output_data))
    print('USERS ANALYTICS TABLE WRITE COMPLETE...')

    # create timestamp column from original timestamp column
    ts_df = filtered_log_df.withColumn("timestamp", F.to_utc_timestamp(F.from_unixtime(F.col('ts')/1000, 'yyyy-MM-dd HH:mm:ss'), 'EST'))
    
    # create datetime column from original timestamp column
    dt_df = ts_df.withColumn("datetime", F.to_date(F.from_unixtime(F.col('ts')/1000)))
    
    # extract columns to create time table
    time_table = dt_df.select(dt_df.datetime.alias('start_time'), 
                hour('timestamp').alias('hour'), 
                dayofmonth('timestamp').alias('day'), 
                weekofyear('timestamp').alias('week'), 
                month('timestamp').alias('month'), 
                year('timestamp').alias('year'), 
                dayofweek('timestamp').alias('weekday')
               ).distinct().sort('start_time')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("ignore").format("parquet").option("compression", "gzip").partitionBy("year", "month").save("{}time_tables.parquet".format(output_data))
    print('TIME ANALYTICS TABLE WRITE COMPLETE...')

    # join song and log datasets and then extract to create songplays table 
    join_cond = [dt_df.artist == songs_df.artist_name, dt_df.song == songs_df.title]
    songplays_table = dt_df\
        .withColumn("songplay_id", monotonically_increasing_id())\
        .join(songs_df, join_cond)\
        .select('songplay_id', dt_df.datetime.alias('start_time'), 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', \
                year('timestamp').alias('year'), month('timestamp').alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").option("compression", "gzip").partitionBy("year", "month").parquet("{}songplays_tables".format(output_data))
    print('SONGPLAYS ANALYTICS TABLE WRITE COMPLETE...')
    
    return songplays_table, users_table, time_table


def test_query(spark, songplays_table, songs_table, artists_table, users_table, time_table):
    """
    Imports Spark DataFrames for the 4 dimensional tables and 1 fact table
    
    Parameters:
    spark        - Spark session that was returned from create_spark_session function.
    songs_table    - Songs dimensional table returned as Spark DataFrame
    artists_table  - Artists dimensional table returned as Spark DataFrame
    users_table        - Users dimensional table returned as Spark DataFrame
    time_table         - Time dimensional table returned as Spark DataFrame
    songplays_table    - Users dimensional table returned as Spark DataFrame
    
    Return:
    Pandas dataframe of queries on the dimensional and fact tables
    """
    
    # creates a temporary view of Spark DataFrames so Spark SQL can be used
    songplays_table.createOrReplaceTempView("songplays_table")
    songs_table.createOrReplaceTempView("songs_table")
    artists_table.createOrReplaceTempView("artists_table")
    
    artist_with_most_music_query = """
    SELECT DISTINCT a.artist_name AS artist_name, SUM(s.duration) total_song_duration
    FROM songplays_table sp
    JOIN songs_table s ON sp.song_id = s.song_id
    JOIN artists_table a ON sp.artist_id = a.artist_id
    GROUP BY a.artist_name
    ORDER BY total_song_duration DESC
    """
    
    all_songs_by_artist_query = """
    SELECT DISTINCT a.artist_name AS artist_name, s.title AS song_title, s.duration as song_length
    FROM songplays_table sp
    JOIN songs_table s ON sp.song_id = s.song_id
    JOIN artists_table a ON sp.artist_id = a.artist_id
    WHERE a.artist_name='The Verve'
    """
    
    all_songs_by_artist_df = spark.sql(all_songs_by_artist_query).toPandas()
    print(all_songs_by_artist_df)
    
    artist_with_most_music_query_df = spark.sql(artist_with_most_music_query).toPandas()
    print(artist_with_most_music_query_df)
    

def main():
    """
    Creates a Spark session, loads in songs and log data, 
    extracts the data for the songs, artists, users, and time dimensional tables, 
    for the songplays fact table, and loads partitioned parquet files into S3 bucket
    
    Parameters:
    N/A
    
    Returns:
    songs_table        - Songs dimensional table returned as Spark DataFrame
    artists_table      - Artists dimensional table returned as Spark DataFrame
    users_table        - Users dimensional table returned as Spark DataFrame
    time_table         - Time dimensional table returned as Spark DataFrame
    songplays_table    - Songplays fact table returned as Spark DataFrame
    """
    bucket = create_bucket()
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-analytics-tables/"
    songs_df, songs_table, artists_table = process_song_data(spark, input_data, output_data)
    songplays_table, users_table, time_table = process_log_data(spark, input_data, output_data, songs_df)
    
    test_query(spark, songplays_table, songs_table, artists_table, users_table, time_table)
    

    # Uncomment to end Spark session when ETL complete
    spark.stop()


if __name__ == "__main__":
    main()
