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

# Script generated for node Customer trusted
Customertrusted_node1745684137361 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="Customertrusted_node1745684137361")

# Script generated for node Accelerometer landing
Accelerometerlanding_node1745684138684 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="Accelerometerlanding_node1745684138684")

# Script generated for node INNER JOIN
SqlQuery1346 = '''
SELECT a.* 
FROM accelerometer_landing a
JOIN customer_trusted c
ON a.user = c.email;
'''
INNERJOIN_node1745684142714 = sparkSqlQuery(glueContext, query = SqlQuery1346, mapping = {"accelerometer_landing":Accelerometerlanding_node1745684138684, "customer_trusted":Customertrusted_node1745684137361}, transformation_ctx = "INNERJOIN_node1745684142714")

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1745684146380 = glueContext.write_dynamic_frame.from_catalog(frame=INNERJOIN_node1745684142714, database="stedi_db", table_name="accelerometer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="Accelerometertrusted_node1745684146380")

job.commit()