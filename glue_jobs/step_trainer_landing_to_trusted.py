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

step_landing_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing"
)

customer_curated_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated"
)

joined_dyf = Join.apply(
    frame1=step_landing_dyf,
    frame2=customer_curated_dyf,
    keys1=["serialNumber"],
    keys2=["serialNumber"]
)


step_trainer_trusted_dyf = SelectFields.apply(
    frame=joined_dyf,
    paths=[
        "sensorReadingTime",
        "serialNumber",
        "distanceFromObject"
    ]
)


sink = glueContext.getSink(
    path="s3://christina-stedi-datalake/trusted/step_trainer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    partitionKeys=[]
)

sink.setFormat("glueparquet")

sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="step_trainer_trusted"
)

sink.writeFrame(step_trainer_trusted_dyf)

job.commit()

