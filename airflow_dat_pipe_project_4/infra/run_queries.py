from pathlib import Path
import psycopg2

sql_file_path = Path.cwd().joinpath("create_tables.sql")

def connect_redshift():
    """
    Connects to Amazon Redshift Serverless using psycopg2.
    Returns a connection object.
    """
    try:
        conn = psycopg2.connect(
            host="redshift-workgroup.294738127702.us-east-1.redshift-serverless.amazonaws.com",
            port=5439,  # default Redshift port
            dbname="dev",
            user="admin",
            password="T3rriblePa$$word!",
        )
        print("Connected to Redshift Serverless")
        return conn
    except Exception as e:
        print("Connection failed:", e)
        return None


try:
    # Read the SQL file
    with open(sql_file_path, "r") as f:
            sql_commands = f.read()

    # Create Redshift connection
    conn = connect_redshift()
    # Run queries
    if conn:
        cur = conn.cursor()
        cur.execute(sql_commands)
        print("SQL file executed successfully")
        conn.autocommit
        cur.close()
        conn.close()
except Exception as e:
    print("Error executing SQL file:", e)

