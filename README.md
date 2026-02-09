# D609 – STEDI Human Balance Analytics - Udacity

## This repository includes an AWS Glue ETL pipeline that builds a Landing, Trusted, and Curated data lake for the STEDI Human Balance Analytics project. Data transformations are checked with Amazon Athena, and screenshots show the results.

## The pipeline uses Amazon S3, AWS Glue Studio, AWS Glue Data Catalog, and Amazon Athena.

# ARCHITECTURE

##S3 Landing Zone
Then it goes to the Trusted Zone, where privacy filtering and schema updates are applied.
Finally, it reaches the Curated Zone, which prepares the data for analytics and machine learning.

## LANDING ZONE

The Landing Zone stores raw JSON data from S3.

Glue jobs used:

* customer_landing_to_trusted.py
* accelerometer_landing_to_trusted.py
* step_trainer_landing_to_trusted.py

I created the landing tables manually in the Glue Console using SQL DDL scripts:

* sql/customer_landing.sql
* sql/accelerometer_landing.sql
* sql/step_trainer_landing.sql

All JSON fields from the source files are included and have the correct data types.

### Athena validation results:

* customer_landing: 956 rows (contains blank shareWithResearchAsOfDate values)
* accelerometer_landing: 81,273 rows
* step_trainer_landing: 28,680 rows

### Screenshots:

* screenshots/customer_landing.png
* screenshots/accelerometer_landing.png
* screenshots/step_trainer_landing.png

## TRUSTED ZONE

The Trusted Zone applies privacy rules and updates schemas as needed.

### Glue jobs used:

* customer_landing_to_trusted.py
* accelerometer_landing_to_trusted.py
* step_trainer_trusted.py

All jobs are set up to update the Glue Data Catalog automatically using these settings:

enableUpdateCatalog=True
updateBehavior=“UPDATE_IN_DATABASE”

Privacy checks:

* customer_trusted contains no blank shareWithResearchAsOfDate values
* accelerometer_trusted contains only data associated with approved customers

### Athena validation results:

* customer_trusted: 482 rows
* accelerometer_trusted: 40,981 rows
* step_trainer_trusted: 14,460 rows

### Screenshots:

* screenshots/customer_trusted.png
* screenshots/accelerometer_trusted.png
* screenshots/step_trainer_trusted.png

## CURATED ZONE

The Curated Zone creates datasets that are ready for analytics and machine learning.

### Customer Curated:

* Glue job: customer_trusted_to_curated.py
* Join: customer_trusted INNER JOIN accelerometer_trusted ON email
* Output table: customer_curated (customer columns only)
* Athena validation: customer_curated = 482 rows
* Screenshot: screenshots/customer_curated.png

### Machine Learning Curated:

* Glue job: machine_learning_curated.py
* Join: step_trainer_trusted INNER JOIN accelerometer_trusted
* Join condition: sensorReadingTime = timestamp
* Output table: machine_learning_curated
* Athena validation: machine_learning_curated = 43,681 rows
* Screenshot: screenshots/machine_learning_curated.png

# REPOSITORY STRUCTURE

D609-STEDI-Udacity/

* glue_jobs/
* sql/
* screenshots/
* README.md



