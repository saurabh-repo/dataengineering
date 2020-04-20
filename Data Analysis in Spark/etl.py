import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit,unix_timestamp, from_unixtime, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R
from pyspark.sql.types import StructField as Fld
from pyspark.sql.types import DoubleType as Dbl
from pyspark.sql.types import StringType as Str 
from pyspark.sql.types import IntegerType as Int
from pyspark.sql.types import DateType as Date
from pyspark.sql.types import LongType as Lgt
from pyspark.sql.types import TimestampType as Tst
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function takes source and target S3 buckets. 
    Parquet files for tables is created at stored in target S3 bucket
    With each invocation of the function, previous files will be overwritten
    '''
    
    #define schema for song data
    songSchema = R([
    Fld("artist_id",       Str()),
    Fld("artist_latitude", Dbl()),
    Fld("artist_location", Str()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_name",     Str()),
    Fld("duration",        Dbl()),
    Fld("num_songs",       Lgt()),
    Fld("song_id",         Str()),
    Fld("title",           Str()),
    Fld("year",            Int())
        ])
    
    #read songs data
    songs_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    #read songs data into a dataframe
    df_song = spark.read.json(songs_data, schema=songSchema)
    
    # extract columns to create songs table
    df_song.createOrReplaceTempView("songs_table")
    songs_table = spark.sql("""
                                SELECT  song_id
                                       ,title
                                       ,artist_id
                                       ,year
                                       ,duration
                                FROM songs_table
                                WHERE song_id IS NOT NULL
                             """)
 
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data+'songs.parquet')

    # extract columns to create artists table
    df_song.createOrReplaceTempView("artists_table")
    artists_table = spark.sql("""
                                 SELECT artist_id
                                       ,artist_name as name
                                       ,artist_location as location
                                       ,artist_latitude as latitude
                                       ,artist_longitude as longitude
                                 FROM  artists_table
                                 WHERE artist_id IS NOT NULL
                              """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists.parquet')



def process_log_data(spark, input_data, output_data):
    '''
    This function takes source and target S3 buckets. 
    Parquet files for dimension and facts is created and stored in target S3 bucket
    With each invocation of the function, previous files will be overwritten
    '''    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
    
    songSchema = R([
    Fld("artist_id",       Str()),
    Fld("artist_latitude", Dbl()),
    Fld("artist_location", Str()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_name",     Str()),
    Fld("duration",        Dbl()),
    Fld("num_songs",       Lgt()),
    Fld("song_id",         Str()),
    Fld("title",           Str()),
    Fld("year",            Int())
        ])
    
    #define schema for log data
    logSchema = R([
    Fld("artist",        Str()),
    Fld("auth"  ,        Str()),
    Fld("firstName",     Str()),
    Fld("gender",        Str()),
    Fld("itemInSession", Str()),
    Fld("lastName",      Str()),
    Fld("length",        Dbl()),
    Fld("level",         Str()),
    Fld("location",      Str()),
    Fld("method",        Str()),
    Fld("page",          Str()),
    Fld("registration",  Lgt()),
    Fld("sessionId",     Int()),
    Fld("song",          Str()),
    Fld("status",        Int()),
    Fld("ts",            Lgt()),
    Fld("userAgent",     Str()),
    Fld("userId",        Int())
        ])
    # read log data file
    #df_log = spark.read.json(log_data, schema=logSchema)
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.userId != '').filter(df_log.page == 'NextSong').filter(df_log.ts != 0)
    
    # extract columns for users table
    df_log.createOrReplaceTempView("users_table")
    users_table = spark.sql("""
                               SELECT userId as user_id, 
                                      firstName as first_name,
                                      lastName as last_name,
                                      gender,
                                      level
                               FROM users_table 
                               WHERE "user_id" IS NOT NULL
                            """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users.parquet')


    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x: int(x/1000), Int())
    get_datetime = udf(lambda x: from_unixtime(x), Tst())

    spark.udf.register('get_timestamp', get_timestamp)
    spark.udf.register('get_datetime', get_datetime)

    df_log = df_log.withColumn('start_time', get_timestamp('ts'))
    df_log = df_log.withColumn("datetime",to_timestamp(df_log["start_time"]).cast('string'))
    df_log = df_log.withColumn("month",month(col("datetime"))).withColumn("year",year(col("datetime")))
    
    df_log.createOrReplaceTempView("time_table")
    time = spark.sql("""
                               SELECT 
                                      datetime  start_time,
                                      hour(datetime) as hour,
                                      dayofmonth(datetime) as day,
                                      weekofyear(datetime) as week,
                                      month(datetime) as month,
                                      year(datetime) as year,
                                      date_format(datetime, 'u') as weekday
                               FROM time_table 
                            """)

    # write time table to parquet files partitioned by year and month
    time.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'time.parquet')    

    # read in song data to use for songplays table
    songs_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    #songs_data = os.path.join(input_data, "song_data")
    df_song = spark.read.json(songs_data, schema=songSchema)
    songs_df = df_song.withColumn("songplay_id", monotonically_increasing_id()).\
    withColumn("song_id",col("song_id")).\
    withColumn("title",col("title")).\
    withColumn("artist_id",col("artist_id")).\
    withColumn("year",col("year")).\
    withColumn("duration",col("duration")).\
    where(col("song_id").isNotNull())

    # extract columns from joined song and log datasets to create songplays table 
    ta = df_log.alias('ta')
    tb = songs_df.alias('tb')
    inner_join = ta.join(tb, (ta.artist == tb.artist_name) & (ta.song == tb.title))
    songplays_table = inner_join.select(tb['songplay_id'],\
                                         ta['datetime'].alias('start_time'),\
                                         ta['userId'].alias('user_id'),\
                                         ta['level'],\
                                         tb['song_id'],\
                                         tb['artist_id'],\
                                         ta['sessionId'].alias('session_id'),\
                                         ta['location'],\
                                         ta['userAgent'].alias('user_agent'),\
                                         year(ta['datetime']).alias('year'),\
                                         month(ta['datetime']).alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'songplays.parquet')

    


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-nano/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
