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


S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="temperature-raw-db",
    table_name="raw_globallandtemperaturesbycountry_csv",
    transformation_ctx="S3bucket_node1",
)

trustedtemperature_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("dt", "string", "date", "date"),
        ("averagetemperature", "double", "averagetemperature", "double"),
        (
            "averagetemperatureuncertainty",
            "double",
            "averagetemperatureuncertainty",
            "double",
        ),
        ("country", "string", "country", "string"),
    ],
    transformation_ctx="trustedtemperature_node2",
)

S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=trustedtemperature_node2,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://jdmejiav-datalake/trusted-temperatures/globallandtemperaturesbycountry/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
