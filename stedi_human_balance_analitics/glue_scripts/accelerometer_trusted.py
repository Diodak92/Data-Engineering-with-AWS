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

# Script generated for node Customer trusted
Customertrusted_node1744581219026 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="Customertrusted_node1744581219026")

# Script generated for node Accelerometer landing
Accelerometerlanding_node1744554463149 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://tomasz-project-stedi-lake-house/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometerlanding_node1744554463149")

# Script generated for node SQL Query
SqlQuery8539 = '''
SELECT a.user,
from_unixtime(a.timestamp / 1000) AS timestamp,
a.x, a.y, a.z
FROM accelerometer_landing a
JOIN customer_trusted c
ON c.email = a.user;
'''
SQLQuery_node1744554471616 = sparkSqlQuery(glueContext, query = SqlQuery8539, mapping = {"accelerometer_landing":Accelerometerlanding_node1744554463149, "customer_trusted":Customertrusted_node1744581219026}, transformation_ctx = "SQLQuery_node1744554471616")

# Script generated for node Accelerometer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1744554471616, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744554030969", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Accelerometertrusted_node1744554474559 = glueContext.getSink(path="s3://tomasz-project-stedi-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometertrusted_node1744554474559")
Accelerometertrusted_node1744554474559.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
Accelerometertrusted_node1744554474559.setFormat("json")
Accelerometertrusted_node1744554474559.writeFrame(SQLQuery_node1744554471616)
job.commit()