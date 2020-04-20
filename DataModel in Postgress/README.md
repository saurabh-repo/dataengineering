# Introduction

The purpose of this project is to built a descision support system(DSS) for the company Spariky.
This DSS will help Spariky to identify customer preferences and provide a better customer experience. 

Currently, the raw data is in JSON format. The scope of the project is to build the Data warehouse schema and ETL pipeline to load data from the JSON files.

The DSS system will help Spariky analytics team to understand the user listening patterns and provide them better recomendations. This is expected to give significant boost to business.

## Database schema design and ETL process

The technology stack used to build the DSS is as below,

1) Python 3.6 used as the prohraming language for buidling data pipleline
2) Data warehouse follows a starschema pattern and has 4 Dimension and 1 fact tables
3) PostgresSQL is used as the backend database

Below is the ER diagram of Spariky Data warehouse.

![ER Diagram](https://r766469c826263xjupyterllyjhwqkl.udacity-student-workspaces.com/lab/tree/ER.PNG)


## Files in repository

1) README.md : Read me file provdiing project description and other details
2) sql_queries.py : SQL file listing all the Create, Inserts statements
3) create_tables.py : Python program to drop and create the tables as listed in sql_queries.py
4) etl.ipynb : Python notebook showing stepwise execution of the ETL process
5) etl.py : ETL program written in python to load data into dimension and fact tables
6) ER.PNG : ER diagram
7) test.ipynb : Python notebook showing sample queries
8) data : directory containing all the data

To run the data pipleine, create_tables.py should be executed first followed by execution of etl.py

```
python create_tables.py
python etl.py
```
