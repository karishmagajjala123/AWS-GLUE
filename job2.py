import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="employee", table_name="employees_csv", transformation_ctx="S3bucket_node1"
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("employee_id", "long", "employee_id", "int"),
        ("first_name", "string", "first_name", "string"),
        ("last_name", "string", "last_name", "string"),
        ("email", "string", "email", "string"),
        ("phone_number", "string", "phone_number", "string"),
        ("hire_date", "string", "hire_date", "string"),
        ("job_id", "string", "job_id", "string"),
        ("salary", "long", "salary", "int"),
        ("commission_pct", "long", "commission_pct", "int"),
        ("manager_id", "long", "manager_id", "int"),
        ("department_id", "long", "department_id", "int"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://cf-etljob/output/", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

job.commit()
