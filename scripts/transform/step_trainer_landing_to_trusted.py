import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def spark_sql_query(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read customer data from curated zone
customer_curated = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mys3nbk/customer/curated"],
        "recurse": True,
    },
    transformation_ctx="customer_curated",
) 

# Read step_trainer data from landing zone
step_trainer_landing = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://mys3nbk/step_trainer/landing"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing",
) 

# Join data from customer_curated with step_trainer_landing
sql_query = """
select DISTINCT step_trainer_landing.* 
from step_trainer_landing
INNER JOIN customer_curated on customer_curated.serialnumber=step_trainer_landing.serialnumber;
"""
joined_data = spark_sql_query(
    glueContext,
    query=sql_query,
    mapping={
        "customer_curated": customer_curated,
        "step_trainer_landing": step_trainer_landing,
    },
    transformation_ctx="joined_data",
)

# Store data to trusted zone
step_trainer_trusted = glueContext.write_dynamic_frame.from_options(
    frame=joined_data,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://mys3nbk/step_trainer/trusted",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted",
)

job.commit()
