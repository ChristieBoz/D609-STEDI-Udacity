import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

step_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted"
).toDF()

accel_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
).toDF()


filtered_steps_df = step_df.join(
    accel_df,
    step_df["sensorreadingtime"] == accel_df["timestamp"],
    how="left_semi"
)

final_df = filtered_steps_df.join(
    accel_df,
    filtered_steps_df["sensorreadingtime"] == accel_df["timestamp"],
    how="inner"
)


ml_df = final_df.select(
    "x",
    "y",
    "z",
    "distancefromobject"
)


ml_dyf = DynamicFrame.fromDF(
    ml_df,
    glueContext,
    "machine_learning_curated_dyf"
)


sink = glueContext.getSink(
    path="s3://christina-stedi-datalake/curated/machine_learning/",
    connection_type="s3",
    enableUpdateCatalog=True
)

sink.setFormat("glueparquet")
sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="machine_learning_curated"
)

sink.writeFrame(ml_dyf)
job.commit()
