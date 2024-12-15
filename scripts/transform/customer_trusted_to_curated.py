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

# Read customer data from trusted zone
customer_trusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://human-balance/customer/trusted"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted",
) 

# Read accelerometer data from landing zone
accelerometer_landing = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://human-balance/accelerometer/landing"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing",
) 

# Join data from customer_trusted with accelerometer_landing
joined_data = Join.apply(
    frame1=accelerometer_landing,
    frame2=customer_trusted,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="joined_data",
)

# Script generated for node Trusted Customer Zone
customer_curated = glueContext.write_dynamic_frame.from_options(
    frame=joined_data,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://human-balance/customer/curated",
        "partitionKeys": [],
        "enableUpdateCatalog": True, 
        "catalog.updateBehavior": "UPDATE_IN_DATABASE"
    },
    transformation_ctx="customer_curated",
)

job.commit()
