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

customer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

accel_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)
joined_dyf = Join.apply(
    frame1=customer_trusted_dyf,
    frame2=accel_trusted_dyf,
    keys1=["email"],
    keys2=["user"]
)
customer_curated_dyf = SelectFields.apply(
    frame=joined_dyf,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber"
    ]
)
sink = glueContext.getSink(
    path="s3://christina-stedi-datalake/curated/customers/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    partitionKeys=[]
)

sink.setFormat("glueparquet")

sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="customer_curated"
)
sink.writeFrame(customer_curated_dyf)

job.commit()

