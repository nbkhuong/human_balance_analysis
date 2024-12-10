import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read customer data from landing zone
customer_landing = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://human-balance/customer/landing"],
        "recurse": True,
    },
    transformation_ctx="customer_landing",
) 

# Filter data with rows agreeing to sharing data for research purpose
filtered_data = Filter.apply(
    frame=customer_landing,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="filtered_data",
)

# Script generated for node Trusted Customer Zone
customer_trusted = glueContext.write_dynamic_frame.from_options(
    frame=filtered_data,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://human-balance/customer/trusted",
        "partitionKeys": [],
    },
    transformation_ctx="customer_trusted",
)

job.commit()
