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
Accelerometertrusted_node1745693639696 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusted_node1745693639696")

# Script generated for node Step trainer trusted
Steptrainertrusted_node1745693640690 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="Steptrainertrusted_node1745693640690")

# Script generated for node INNER JOIN
SqlQuery1144 = '''
SELECT DISTINCT a.user, s.serialnumber, a.timestamp, 
s.distanceFromObject, a.x, a.y, a.z
FROM step_trainer_trusted s
JOIN accelerometer_trusted a
ON s.sensorreadingtime = a.timestamp;
'''
INNERJOIN_node1745693645538 = sparkSqlQuery(glueContext, query = SqlQuery1144, mapping = {"step_trainer_trusted":Steptrainertrusted_node1745693640690, "accelerometer_trusted":Accelerometertrusted_node1745693639696}, transformation_ctx = "INNERJOIN_node1745693645538")

# Script generated for node Machine learning curated
Machinelearningcurated_node1745693648718 = glueContext.write_dynamic_frame.from_catalog(frame=INNERJOIN_node1745693645538, database="stedi_db", table_name="machine_learning_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="Machinelearningcurated_node1745693648718")

job.commit()