import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config['IAM_ROLE']['DWH_ROLE_ARN']
S3_LOG_DATA_PATH = config['S3']['LOG_DATA']
S3_SONG_DATA_PATH = config['S3']['SONG_DATA']
LOG_JSON_PATH = config['S3']['LOG_JSON_PATH']

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE STAGING TABLES
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
    DISTSTYLE ALL
    SORTKEY (ts);
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
    DISTSTYLE ALL
    SORTKEY (song_id);
""")

# CREATE FACT TABLES
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
    DISTSTYLE ALL
    SORTKEY (start_time);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        gender CHAR,
        level VARCHAR(4)
    )
    DISTKEY (user_id)
    SORTKEY (last_name);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(50) PRIMARY KEY,
        title VARCHAR(100),
        artist_id VARCHAR(50) NOT NULL,
        year INT,
        duration FLOAT
    )
    DISTKEY (song_id)
    SORTKEY (title);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        location VARCHAR(100),
        latitude FLOAT,
        longitude FLOAT
    )
    DISTKEY (artist_id)
    SORTKEY (name);
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
    DISTSTYLE ALL
    SORTKEY (start_time);
""")

# INSERT INTO STAGING TABLES
staging_events_copy = (f"""
    COPY staging_events
    FROM '{S3_LOG_DATA_PATH}'
    IAM_ROLE '{DWH_ROLE_ARN}'
    REGION 'us-west-2'
    FORMAT AS JSON '{LOG_JSON_PATH}'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL
    STATUPDATE OFF
    COMPUPDATE OFF;
""")

staging_songs_copy = (f"""
    COPY staging_songs
    FROM '{S3_SONG_DATA_PATH}'
    IAM_ROLE '{DWH_ROLE_ARN}'
    REGION 'us-west-2'
    JSON 'auto'
    BLANKSASNULL
    EMPTYASNULL
    TRUNCATECOLUMNS
    STATUPDATE OFF
    COMPUPDATE OFF;
""")

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS START_TIME,
    e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
    FROM staging_events e
    JOIN staging_songs s
    ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT TIMESTAMP AS START_TIME, EXTRACT(HOUR FROM TIMESTAMP) AS hour,
    EXTRACT(DAY FROM TIMESTAMP) AS day, EXTRACT(WEEK FROM TIMESTAMP) AS week,
    EXTRACT(MONTH FROM TIMESTAMP) AS month, EXTRACT(YEAR FROM TIMESTAMP) AS year,
    EXTRACT(WEEKDAY FROM TIMESTAMP) AS weekday
    FROM(
        SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS TIMESTAMP
        FROM staging_events
        WHERE ts IS NOT NULL
        AND page = 'NextSong'
        );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
