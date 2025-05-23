import boto3
import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def drop_tables(cur):
    """Drop existing tables in the database."""
    logger.info("Dropping existing tables...")
    for query in drop_table_queries:
        logger.info(f"Executing query: {query}")
        cur.execute(query)
    logger.info("Dropped existing tables successfully.")


def create_tables(cur):
    """Create new tables in the database."""
    logger.info("Creating new tables...")
    for query in create_table_queries:
        logger.info(f"Executing query: {query}")
        cur.execute(query)
    logger.info("Created new tables successfully.")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    AWS_KEY = config.get("AWS","KEY")
    AWS_SECRET = config.get("AWS","SECRET")
    AWS_REGION = config.get("AWS","REGION")

    CLUSTER_IDENTIFIER = config.get("REDSHIFT_CLUSTER", "CLUSTER_IDENTIFIER")
    DB_NAME = config.get("REDSHIFT_CLUSTER", "DB_NAME")
    DB_USER = config.get("REDSHIFT_CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("REDSHIFT_CLUSTER", "DB_PASSWORD")
    DB_PORT = config.get("REDSHIFT_CLUSTER", "DB_PORT")

    # Initialize a session using Amazon Redshift
    redshift_client = boto3.client(
        'redshift',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
        )

    myClusterProps = redshift_client.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_HOST = myClusterProps['Endpoint']['Address']

    conn = psycopg2.connect(f"host={DWH_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
    logger.info("Connected to Redshift cluster successfully.")
    conn.autocommit = True
    cur = conn.cursor()
    drop_tables(cur)
    create_tables(cur)
    conn.close()


if __name__ == "__main__":
    main()