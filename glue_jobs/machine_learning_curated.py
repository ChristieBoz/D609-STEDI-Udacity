import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import col, floor

# Read JOB_NAME from args
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Set up contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Read trusted step trainer data
step_trainer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted"
)

# 2. Read trusted accelerometer data
accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)

# 3. Convert to DataFrames and create "time_sec" (timestamp rounded to seconds)
step_df = step_trainer_trusted_dyf.toDF().withColumn(
    "time_sec",
    (col("sensorreadingtime").cast("bigint") / 1000).cast("bigint")
)

accel_df = accelerometer_trusted_dyf.toDF().withColumn(
    "time_sec",
    (col("timestamp").cast("bigint") / 1000).cast("bigint")
)

# 4. Join on time_sec
joined_df = step_df.join(
    accel_df,
    on="time_sec",
    how="inner"
)

# 5. Select the ML fields we care about
ml_df = joined_df.select(
    col("sensorreadingtime"),
    col("serialnumber"),
    col("distancefromobject"),
    col("x"),
    col("y"),
    col("z")
)

# 6. Convert back to DynamicFrame
ml_curated_dyf = DynamicFrame.fromDF(
    ml_df,
    glueContext,
    "ml_curated_dyf"
)

# 7. Sink: write to S3 and update Glue Catalog
sink = glueContext.getSink(
    path="s3://christina-stedi-datalake/curated/machine_learning/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    partitionKeys=[]
)

sink.setFormat("glueparquet")

sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="machine_learning_curated"
)

sink.writeFrame(ml_curated_dyf)

job.commit()
