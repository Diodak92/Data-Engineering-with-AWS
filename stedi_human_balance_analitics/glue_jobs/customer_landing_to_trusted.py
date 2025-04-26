import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Customer landing
Customerlanding_node1745683774079 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_landing", transformation_ctx="Customerlanding_node1745683774079")

# Script generated for node Trusted filter
SqlQuery1276 = '''
SELECT * 
FROM customer_landing
WHERE shareWithResearchasOfDate IS NOT NULL
OR shareWithResearchasOfDate != '';
'''
Trustedfilter_node1745683781019 = sparkSqlQuery(glueContext, query = SqlQuery1276, mapping = {"customer_landing":Customerlanding_node1745683774079}, transformation_ctx = "Trustedfilter_node1745683781019")

# Script generated for node Customer trusted
Customertrusted_node1745683784930 = glueContext.write_dynamic_frame.from_catalog(frame=Trustedfilter_node1745683781019, database="stedi_db", table_name="customer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="Customertrusted_node1745683784930")

job.commit()