import configparser
import psycopg2
import logging
import boto3
from sql_queries import copy_table_queries, insert_table_queries

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_staging_tables(cur):
    """Load data into staging tables from S3."""
    logger.info("Loading data into staging tables...")
    for query in copy_table_queries:
        logger.info(f"Executing query: {query}")
        cur.execute(query)
    logger.info("Data loaded into staging tables successfully.")


def insert_tables(cur):
    """Insert data into final tables from staging tables."""
    logger.info("Inserting data into final tables...")
    for query in insert_table_queries:
        logger.info(f"Executing query: {query}")
        cur.execute(query)
    logger.info("Data inserted into final tables successfully.")


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
    #  Create a cursor object
    conn.autocommit = True
    cur = conn.cursor()
    load_staging_tables(cur)
    insert_tables(cur)
    conn.close()


if __name__ == "__main__":
    main()