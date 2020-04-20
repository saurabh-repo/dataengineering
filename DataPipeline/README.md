# Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines 
and come to the conclusion that the best tool to achieve this is Apache Airflow.

Scope of the project is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. 
The data quality plays a big part when analyses are executed on top the data warehouse.
The ETL steps should be followed by a data validation to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. 
The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Data lake schema design and ETL process

The technology stack used to build the DSS is as below,

1) Airflow DAGs have been used to orchestrate the ETL Data pipline.
2) Source data is stored in AWS S3.
3) Data pipeline flow is as below
   a)Create the staging, dimension and fact tables in Redshift
   b)Copy the data from S3 to staging tables - staging_events and staging_songs
   c)Once data is successfully copied to staging tables,trigger insert to dimenion and fact tables
   d)At the end of data pipeline, peform data validation.   
5) AWS 2 nodes Redshift using dc2.large have been used.


## Files in repository

1) README.md : Read me file provdiing project description and other details
2) Final Data Pipeline.PNG : Image showing the Data pipeline 

In the current implementation, data pipeline should be manually triggered. Depending on the requirements, start,end time and schedule can be updated.