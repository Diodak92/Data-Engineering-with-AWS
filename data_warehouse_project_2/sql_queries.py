import configparser
import os

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config['IAM_ROLE']['DWH_ROLE_ARN']
S3_LOG_DATA_PATH = config['S3']['LOG_DATA']
S3_SONG_DATA_PATH = config['S3']['SONG_DATA']
LOG_JSON_PATH = config['S3']['LOG_JSON_PATH']

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR(100),
        auth VARCHAR(50),
        firstName VARCHAR(50),
        gender CHAR,
        itemInSession INT,
        lastName VARCHAR(50),
        length FLOAT,
        level VARCHAR(4),
        location VARCHAR(100),
        method VARCHAR(3),
        page VARCHAR(20),
        registration FLOAT,
        sessionId INT,
        song VARCHAR(100),
        status INT,
        ts BIGINT,
        userAgent VARCHAR(200),
        userId INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id VARCHAR(50),
        artist_latitude FLOAT,
        artist_location VARCHAR(100),
        artist_longitude FLOAT,
        artist_name VARCHAR(100),
        duration FLOAT,
        num_songs INT,
        song_id VARCHAR(50),
        title VARCHAR(100),
        year INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR(4) NOT NULL,
        song_id VARCHAR(50) NOT NULL,
        artist_id VARCHAR(50) NOT NULL,
        session_id INT NOT NULL,
        location VARCHAR(100),
        user_agent VARCHAR(200)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender CHAR,
        level VARCHAR(4)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(50) PRIMARY KEY,
        title VARCHAR(100),
        artist_id VARCHAR(50) NOT NULL,
        year INT,
        duration FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        location VARCHAR(100),
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday VARCHAR(10)
    )
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events
    FROM '{S3_LOG_DATA_PATH}'
    IAM_ROLE '{DWH_ROLE_ARN}'
    REGION 'us-west-2'
    FORMAT AS JSON '{LOG_JSON_PATH}'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""")

staging_songs_copy = (f"""
    COPY staging_songs
    FROM '{S3_SONG_DATA_PATH}'
    IAM_ROLE '{DWH_ROLE_ARN}'
    REGION 'us-west-2'
    JSON 'auto'
""")

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
