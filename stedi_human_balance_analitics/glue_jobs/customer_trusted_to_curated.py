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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1745684461234 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusted_node1745684461234")

# Script generated for node Customer trusted
Customertrusted_node1745684462373 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="Customertrusted_node1745684462373")

# Script generated for node INNER JOIN
SqlQuery1234 = '''
SELECT DISTINCT c.* 
FROM customer_trusted c
JOIN accelerometer_trusted a
ON c.email = a.user;
'''
INNERJOIN_node1745684465187 = sparkSqlQuery(glueContext, query = SqlQuery1234, mapping = {"customer_trusted":Customertrusted_node1745684462373, "accelerometer_trusted":Accelerometertrusted_node1745684461234}, transformation_ctx = "INNERJOIN_node1745684465187")

# Script generated for node Customer curated
Customercurated_node1745684467623 = glueContext.write_dynamic_frame.from_catalog(frame=INNERJOIN_node1745684465187, database="stedi_db", table_name="customer_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="Customercurated_node1745684467623")

job.commit()