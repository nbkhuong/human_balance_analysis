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

# Read accelerometer data from trusted zone
accelerometer_trusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://human-balance/accelerometer/trusted"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted",
) 

# Read step_trainer data from trusted zone
step_trainer_trusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://human-balance/step_trainer/trusted"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted",
) 

# Join data from customer_trusted with step_trainer_trusted
sql_query = """
select DISTINCT * 
from step_trainer_trusted
INNER JOIN accelerometer_trusted on accelerometer_trusted.timestamp=step_trainer_trusted.sensorreadingtime;
"""
joined_data = spark_sql_query(
    glueContext,
    query=sql_query,
    mapping={
        "accelerometer_trusted": accelerometer_trusted,
        "step_trainer_trusted": step_trainer_trusted,
    },
    transformation_ctx="joined_data",
)
# Drop unnecessary fields
dropped_data = DropFields.apply(
    frame=joined_data,
    paths=[
        "timestamp",
        "user",
        "x",
        "y",
        "z"
    ],
    transformation_ctx="joined_data",
)

# Store data to trusted zone
machine_learning_curated = glueContext.write_dynamic_frame.from_options(
    frame=dropped_data,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://human-balance/step_trainer/curated",
        "partitionKeys": [],
    },
    transformation_ctx="machine_learning_curated",
)

job.commit()
