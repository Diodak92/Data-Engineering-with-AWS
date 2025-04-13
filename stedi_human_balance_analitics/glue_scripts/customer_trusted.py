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

# Script generated for node Customer landing
Customerlanding_node1744548099603 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "true"}, connection_type="s3", format="json", connection_options={"paths": ["s3://tomasz-project-stedi-lake-house/customer/landing/"], "recurse": True}, transformation_ctx="Customerlanding_node1744548099603")

# Script generated for node Customer filter
SqlQuery9012 = '''
-- {"customerName": "Santosh Clayton", "email": "Santosh.Clayton@test.com", "phone": "8015551212", "birthDay": "1900-01-01", "serialNumber": "50f7b4f3-7af5-4b07-a421-7b902c8d2b7c",
-- "registrationDate": 1655564376361, "lastUpdateDate": 1655564376361, "shareWithResearchAsOfDate": 1655564376361, "shareWithPublicAsOfDate": 1655564376361, "shareWithFriendsAsOfDate": 

SELECT *
FROM (
    SELECT customerName, email, phone, CAST(birthDay AS DATE) AS birthDay, serialNumber, 
    from_unixtime(registrationDate / 1000) AS registrationDate, 
    from_unixtime(lastUpdateDate / 1000) AS lastUpdateDate, 
    from_unixtime(shareWithResearchAsOfDate / 1000) AS shareWithResearchAsOfDate,
    from_unixtime(shareWithPublicAsOfDate / 1000) AS shareWithPublicAsOfDate,
    from_unixtime(shareWithFriendsAsOfDate / 1000) AS shareWithFriendsAsOfDate
    FROM customer_trusted
    )
WHERE shareWithResearchAsOfDate IS NOT NULL OR shareWithResearchAsOfDate != ''
AND birthDay IS NOT NULL OR birthDay != '';
'''
Customerfilter_node1744548110342 = sparkSqlQuery(glueContext, query = SqlQuery9012, mapping = {"customer_trusted":Customerlanding_node1744548099603}, transformation_ctx = "Customerfilter_node1744548110342")

# Script generated for node Customer trusted
EvaluateDataQuality().process_rows(frame=Customerfilter_node1744548110342, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1744548071314", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customertrusted_node1744548114250 = glueContext.getSink(path="s3://tomasz-project-stedi-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customertrusted_node1744548114250")
Customertrusted_node1744548114250.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
Customertrusted_node1744548114250.setFormat("json")
Customertrusted_node1744548114250.writeFrame(Customerfilter_node1744548110342)
job.commit()