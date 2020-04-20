# Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the AWS cloud. Their data resides in AWS S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to built a descision support system(DSS) hosted using AWS offerings.
This DSS will help Spariky to identify customer preferences and provide a better customer experience. 

The DSS system will help Spariky analytics team to understand the user listening patterns and provide them better recomendations. This is expected to give significant boost to business.

## Database schema design and ETL process

The technology stack used to build the DSS is as below,

1) Python 3.6 used as the prohraming language for buidling data pipleline
2) Data warehouse follows a starschema pattern and has 4 Dimension and 1 fact tables. In addition to this there are two staging tables.
   Staging tables are the landing zone for the data imprted from AWS S3 bucket.
3) For improved performance,Primary key of tables users,songs, artists have been selected as distribution and sort key.
   Dimenion table time has been cretaed with diststyle all and fact table songplays uses user_id as the distribution and sort key.
3) AWS Redshift is used as the backend data warehouse database

Below is the ER diagram of Spariky Data warehouse.

![ER Diagram](https://r766469c826419xjupyterlr5tapor7.udacity-student-workspaces.com/files/EDW%20Data%20Model.PNG?_xsrf=2%7C7183c7eb%7C7111475dacfb67b8403da7faaeee05f4%7C1585615198)


## Files in repository

1) README.md : Read me file provdiing project description and other details
2) sql_queries.py : SQL file listing all the Create, Inserts statements
3) create_tables.py : Python program to drop and create the tables as listed in sql_queries.py
4) etl.py : ETL program written in python to load data into dimension and fact tables
5) EDW Data Model.PNG : ER diagram
7) test.ipynb : Python notebook showing sample queries
8) dwh.cfg : Configuration file containing Redshift Cluster Connection Parameters Password has been obfuscated

To run the data pipleine, create_tables.py should be executed first followed by execution of etl.py

```
python create_tables.py
python etl.py
```
