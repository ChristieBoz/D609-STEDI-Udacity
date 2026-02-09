# D609-STEDI-Udacity
D609 WGU Udacity Project


This project implements a full AWS Glue-based ETL pipeline for the STEDI Human Balance Analytics system. The pipeline ingests raw sensor data, applies privacy filtering, and produces curated datasets suitable for machine learning.

The architecture follows a Landing >  Trusted >  Curated data lake pattern using:

Amazon S3
AWS Glue Studio
AWS Glue Data Catalog
Amazon Athena

Architecture Overview

Data Flow:

Each zone is validated using Amazon Athena queries, with screenshots included as evidence.

Landing Zone

Purpose

The Landing Zone contains raw JSON data ingested directly from S3 without transformations.

Data Sources

Customer data
Accelerometer data
Step trainer data

Glue Jobs

The following Glue jobs ingest data from S3 into landing tables:

customer_landing_to_trusted.py
accelerometer_landing_to_trusted.py
step_trainer_landing_to_trusted.py



Each job connects directly to the appropriate S3 bucket.

Glue Tables 

Landing tables were created in the Glue Console using SQL DDL scripts:

sql/customer_landing.sql
sql/accelerometer_landing.sql
sql/step_trainer_landing.sql

 

All JSON fields from the input files are included and appropriately typed

Athena Validation

Athena queries were run to validate the data:

 

Additional validation proves that customer_landing contains rows with blank shareWithResearchAsOfDate.

 

Screenshots are included in the /screenshots directory.

 

Trusted Zone


The Trusted Zone removes data that does not meet privacy requirements and ensures schema consistency.

Schema Updates

All Glue jobs in the Trusted Zone are configured to:

Create tables in the Glue Data Catalog
Dynamically infer and update schema
Update existing tables on subsequent runs

 

This is enabled using:

 

enableUpdateCatalog=True

updateBehavior="UPDATE_IN_DATABASE"

 
