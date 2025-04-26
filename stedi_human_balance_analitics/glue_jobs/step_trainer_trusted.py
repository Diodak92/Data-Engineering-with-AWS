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

# Script generated for node Step trainer landing
Steptrainerlanding_node1745692724201 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landing", transformation_ctx="Steptrainerlanding_node1745692724201")

# Script generated for node Customer curated
Customercurated_node1745692723479 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_curated", transformation_ctx="Customercurated_node1745692723479")

# Script generated for node INNER JOIN
SqlQuery1056 = '''
SELECT s.* 
FROM step_trainer_landing s
JOIN customer_curated c
ON s.serialnumber = c.serialnumber;
'''
INNERJOIN_node1745692726862 = sparkSqlQuery(glueContext, query = SqlQuery1056, mapping = {"step_trainer_landing":Steptrainerlanding_node1745692724201, "customer_curated":Customercurated_node1745692723479}, transformation_ctx = "INNERJOIN_node1745692726862")

# Script generated for node Step trainer trusted
Steptrainertrusted_node1745692728849 = glueContext.write_dynamic_frame.from_catalog(frame=INNERJOIN_node1745692726862, database="stedi_db", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="Steptrainertrusted_node1745692728849")

job.commit()