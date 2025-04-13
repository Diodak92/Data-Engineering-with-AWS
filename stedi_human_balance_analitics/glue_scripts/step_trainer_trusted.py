import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Step trainer landing
Steptrainerlanding_node1744572288658 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://tomasz-project-stedi-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="Steptrainerlanding_node1744572288658")

# Script generated for node Customer curated
Customercurated_node1744572284177 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://tomasz-project-stedi-lake-house/customer/curated/"], "recurse": True}, transformation_ctx="Customercurated_node1744572284177")

# Script generated for node SQL Query
SqlQuery8410 = '''
SELECT from_unixtime(s.sensorReadingTime / 1000) AS sensorReadingTime,
s.serialNumber, s.distanceFromObject 
FROM step_trainer_landing s
JOIN customer_curated c
ON s.serialNumber = c.serialNumber;
'''
SQLQuery_node1744572292980 = sparkSqlQuery(glueContext, query = SqlQuery8410, mapping = {"step_trainer_landing":Steptrainerlanding_node1744572288658, "customer_curated":Customercurated_node1744572284177}, transformation_ctx = "SQLQuery_node1744572292980")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1744572292980, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744566694575", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1744572303988 = glueContext.getSink(path="s3://tomasz-project-stedi-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1744572303988")
AmazonS3_node1744572303988.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
AmazonS3_node1744572303988.setFormat("json")
AmazonS3_node1744572303988.writeFrame(SQLQuery_node1744572292980)
job.commit()