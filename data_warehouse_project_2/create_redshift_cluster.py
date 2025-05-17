import boto3
import configparser
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))


AWS_KEY = config.get("AWS","KEY")
AWS_SECRET = config.get("AWS","SECRET")
AWS_REGION = config.get("AWS","REGION")

DWH_ROLE_ARN = config.get("IAM_ROLE", "DWH_ROLE_ARN")
VPC_SECURITY_GROUP_ID = config.get("VPC", "VPC_SECURITY_GROUP_ID")

CLUSTER_IDENTIFIER = config.get("REDSHIFT_CLUSTER", "CLUSTER_IDENTIFIER")
CLUSTER_TYPE = config.get("REDSHIFT_CLUSTER", "CLUSTER_TYPE")
NODE_TYPE = config.get("REDSHIFT_CLUSTER", "NODE_TYPE") 
NODE_NUMBER = config.get("REDSHIFT_CLUSTER", "NUM_NODES")
DB_NAME = config.get("REDSHIFT_CLUSTER", "DB_NAME")
DB_USER = config.get("REDSHIFT_CLUSTER", "DB_USER")
DB_PASSWORD = config.get("REDSHIFT_CLUSTER", "DB_PASSWORD")
DB_PORT = config.get("REDSHIFT_CLUSTER", "DB_PORT")

logger.info("Configuration loaded successfully.")

# Initialize a session using Amazon Redshift
redshift_client = boto3.client(
    'redshift',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET
    )

ec2 = boto3.resource('ec2',
                     region_name=AWS_REGION,
                     aws_access_key_id=AWS_KEY,
                     aws_secret_access_key=AWS_SECRET
                     )


def check_cluster_status(cluster_identifier : str) -> str:
    try:
        response = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)
        cluster_status = response['Clusters'][0]['ClusterStatus']
        return cluster_status
    except Exception as e:
        logger.error(f"Error checking cluster status: {e}")
        return None
# Check if the cluster already exists

cluster_status = check_cluster_status(CLUSTER_IDENTIFIER)
if cluster_status:
    logger.info(f"Cluster {CLUSTER_IDENTIFIER} already exists with status: {cluster_status}")
else:
    logger.info("Cluster is not available.")
    # Create a Redshift cluster
    logger.info("Creating a new Redshift cluster...")
    try:
        response = redshift_client.create_cluster(
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NODE_NUMBER),
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            Port=int(DB_PORT),
            PubliclyAccessible=True,
            IamRoles=[DWH_ROLE_ARN]
            )
        while check_cluster_status(CLUSTER_IDENTIFIER) != 'available':
            logger.info(check_cluster_status(CLUSTER_IDENTIFIER))
            time.sleep(10)
        logger.info("Redshift cluster created successfully.")
    except Exception as e:
        logger.error(f"Error creating Redshift cluster: {e}")
        raise

    # Open the port for incoming connections
    try:
        myClusterProps = redshift_client.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        logger.info(defaultSg)
                
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name, 
            CidrIp='0.0.0.0/0',  
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
            )
    except Exception as e:
        logger.error(e)
