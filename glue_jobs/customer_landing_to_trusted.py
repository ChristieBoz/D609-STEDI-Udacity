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

customer_landing_dyf = glueContext.create_dynamic_frame.from_catalog(
  database="stedi",
  table_name="customer_landing"
)

customer_trusted_dyf = Filter.apply(
    frame=customer_landing_dyf,
    f=lambda row: row["sharewithresearchasofdate"] is not None
                  and row["sharewithresearchasofdate"] != ""
)                

sink = glueContext.getSink(
    path="s3://christina-stedi-datalake/trusted/customer/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    partitionKeys=[]
)

sink.setFormat("glueparquet")

sink.setCatalogInfo(
    catalogDatabase="stedi",
    catalogTableName="customer_trusted"
)

sink.writeFrame(customer_trusted_dyf)

job.commit()
