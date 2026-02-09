# D609 – STEDI Human Balance Analytics Udacity

This repository provides an AWS Glue ETL pipeline that sets up Landing, Trusted, and Curated zones for the STEDI Human Balance Analytics project. Data transformations are checked with Amazon Athena, and screenshots show the results.

The pipeline uses Amazon S3, AWS Glue Studio, AWS Glue Data Catalog, and Amazon Athena.

## Landing Zone

Glue Jobs

These Glue Studio jobs load raw JSON data from S3 into the Landing Zone:

* customer_landing_to_trusted.py
* accelerometer_landing_to_trusted.py
* step_trainer_landing_to_trusted.py

SQL Tables 

Landing tables were set up manually in the Glue Console with SQL DDL scripts:

* sql/customer_landing.sql
* sql/accelerometer_landing.sql
* sql/step_trainer_landing.sql

All fields from the source JSON data are included and have the correct data types.

Athena Validation

* customer_landing: 956 rows
* accelerometer_landing: 81,273 rows
* step_trainer_landing: 28,680 rows

Screenshots:

* screenshots/customer_landing.png
* screenshots/accelerometer_landing.png
* screenshots/step_trainer_landing.png

## Trusted Zone

Glue Jobs

These jobs are set up to update the Glue Data Catalog schema automatically:

* customer_landing_to_trusted.py
* accelerometer_landing_to_trusted.py
* step_trainer_trusted.py

These jobs have the following settings:

enableUpdateCatalog=True
updateBehavior="UPDATE_IN_DATABASE"

Athena Validation

* customer_trusted: 482 rows
* accelerometer_trusted: 40,981 rows
* step_trainer_trusted: 14,460 rows

These results show that:

* customer_trusted has no blank shareWithResearchAsOfDate values.

Screenshots:

* screenshots/customer_trusted.png
* screenshots/accelerometer_trusted.png
* screenshots/step_trainer_trusted.png

## Curated Zone

customer_curated

Glue Job: customer_trusted_to_curated.py
Join: customer_trusted INNER JOIN accelerometer_trusted ON email
Result: customer_curated (customer columns only)
Athena Validation: 482 rows
Screenshot: screenshots/customer_curated.png

machine_learning_curated

Glue Job: machine_learning_curated.py
Join: step_trainer_trusted INNER JOIN accelerometer_trusted
Condition: sensorReadingTime = timestamp
Result: machine_learning_curated
Athena Validation: 43,681 rows
Screenshot: screenshots/machine_learning_curated.png

Repository Structure

D609-STEDI-Udacity/
├── glue_jobs/
├── sql/
├── screenshots/
└── README.md

