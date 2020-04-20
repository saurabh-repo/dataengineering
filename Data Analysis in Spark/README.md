# Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to build a data lake in AWS S3. An ETL pipeline will extract the data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional and fact tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

The data lake system will help Spariky analytics team to process large amount of data and gain insights to end customers listening patterns and provide them better recomendations. This is expected to give significant boost to business.

## Data lake schema design and ETL process

The technology stack used to build the DSS is as below,

1) Pyspark used as the programing language for buidling data pipleline
2) Spark version 2.7.5 has been used.
3) Data lake will consists of data of 4 Dimension and 1 Fact tables in parquet format.
   Below is the partition scheme used for dimension and fact tables
   a) songs - partition by 'year','artist_id'
   b) time -  partition by 'year','month'
   c) songplays - partition by 'year','month'
4) The bucket with parquet files will look as below,
   ![Data Lake](https://r766469c826427xjupyterlx8knk8rj.udacity-student-workspaces.com/files/S3%20Buckets.PNG?_xsrf=2%7Ca85141f2%7C1a22a0cf7627fe040c9fab41a7176cf5%7C1585616605)
5) AWS 3 node EMR cluster using m4.large instances has been used to process the data.

## Files in repository

1) README.md : Read me file provdiing project description and other details
2) etl.py : ETL program written in python to load data into dimension and fact tables
3) S3 Buckets.PNG : List of parquet files generated as a end result
4) dl.cfg : Configuration file containing Redshift Cluster Connection Parameters.Credentials have been obfuscated

To run the data pipleine, 

```
python etl.py
```
