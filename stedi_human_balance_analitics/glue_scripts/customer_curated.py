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
Steptrainerlanding_node1744568552850 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://tomasz-project-stedi-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="Steptrainerlanding_node1744568552850")

# Script generated for node Customer trusted
Customertrusted_node1744568555885 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://tomasz-project-stedi-lake-house/customer/trusted/"], "recurse": True}, transformation_ctx="Customertrusted_node1744568555885")

# Script generated for node SQL Query
SqlQuery8106 = '''
SELECT customerName, email, phone, birthday, serialnumber, registrationdate, lastupdatedate,
sharewithresearchasofdate, sharewithpublicasofdate, sharewithfriendsasofdate
FROM (
    SELECT c.*, 
    ROW_NUMBER() OVER(PARTITION BY c.email ORDER BY lastupdatedate) AS RN
    FROM customer_trusted c
    JOIN step_trainer_landing s
    ON c.serialNumber = s.serialNumber
    )
WHERE RN = 1;
'''
SQLQuery_node1744568567092 = sparkSqlQuery(glueContext, query = SqlQuery8106, mapping = {"customer_trusted":Customertrusted_node1744568555885, "step_trainer_landing":Steptrainerlanding_node1744568552850}, transformation_ctx = "SQLQuery_node1744568567092")

# Script generated for node Customers curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1744568567092, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744566694575", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customerscurated_node1744568586376 = glueContext.getSink(path="s3://tomasz-project-stedi-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customerscurated_node1744568586376")
Customerscurated_node1744568586376.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customers_curated")
Customerscurated_node1744568586376.setFormat("json")
Customerscurated_node1744568586376.writeFrame(SQLQuery_node1744568567092)
job.commit()