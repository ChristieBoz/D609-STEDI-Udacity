import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

step_trainer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted"
)

accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)

customer_curated_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated"
)

step_accel_dyf = Join.apply(
    frame1=step_trainer_trusted_dyf,
    frame2=accelerometer_trusted_dyf,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"]
)


ml_joined_dyf = Join.apply(
    frame1=step_accel_dyf,
    frame2=customer_curated_dyf,
    keys1=["serialnumber"],
    keys2=["serialNumber"]
)

ml_curated_dyf = SelectFields.apply(
    frame=ml_joined_dyf,
    paths=[
        "sensorreadingtime",
        "serialnumber",
        "distancefromobject",
        "x",
        "y",
        "z",
        "email"
    ]
)

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

