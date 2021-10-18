import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
ARN = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_data"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS events (
        event_id BIGINT IDENTITY(0,1) NOT NULL,
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length DECIMAL,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INTEGER NOT NULL SORTKEY DISTKEY,
        song VARCHAR,
        status INTEGER,
        ts BIGINT NOT NULL,
        userAgent VARCHAR,
        userId INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_data (
        num_songs INTEGER NOT NULL, 
        artist_id VARCHAR NOT NULL SORTKEY DISTKEY,
        artist_latitude DECIMAL, 
        artist_longitude DECIMAL,
        artist_location VARCHAR, 
        artist_name VARCHAR, 
        song_id VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        duration DECIMAL, 
        year INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1) SORTKEY,
        start_time TIMESTAMP NOT NULL, 
        user_id INTEGER NOT NULL DISTKEY, 
        level VARCHAR, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id INTEGER, 
        location VARCHAR, 
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER NOT NULL SORTKEY,
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR SORTKEY,
        title VARCHAR, 
        artist_id VARCHAR, 
        year INTEGER, 
        duration DECIMAL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR SORTKEY,
        name VARCHAR, 
        location VARCHAR, 
        latitude DECIMAL, 
        longitude DECIMAL
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP SORTKEY, 
        hour INTEGER, 
        day INTEGER, 
        week INTEGER, 
        month INTEGER, 
        year INTEGER, 
        weekday INTEGER
    )
""")


# STAGING TABLES

staging_events_copy = ("""
    copy events from {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy song_data from {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT  DISTINCT 
    
            TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second'   AS start_time,
            e.userId     AS user_id,
            e.level      AS level,
            sd.song_id   AS song_id,
            sd.artist_id AS artist_id,
            e.sessionId  AS session_id,
            e.location   AS location,
            e.userAgent  AS user_agent
            
    FROM events AS e
    JOIN song_data AS sd ON e.artist = sd.artist_name
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT  DISTINCT 
    
        userId       AS user_id,
        firstName    AS first_name,
        lastName     AS last_name,
        gender       AS gender,
        level        AS level
        
    FROM events
    WHERE page = 'NextSong'
    ;
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    
    SELECT  DISTINCT 
        
        song_id      AS song_id,
        title        AS title,
        artist_id    AS artist_id,
        year         AS year,
        duration     AS duration
        
    FROM song_data;
""")


artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    
    SELECT  DISTINCT 
    
        artist_id           AS artist_id,
        artist_name         AS name,
        artist_location     AS location,
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
        
    FROM song_data;
""")



time_table_insert = ("""
    INSERT INTO time ( 
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    
    SELECT DISTINCT 
        
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time)    AS hour,
        EXTRACT(day FROM start_time)     AS day,
        EXTRACT(week FROM start_time)    AS week,
        EXTRACT(month FROM start_time)   AS month,
        EXTRACT(year FROM start_time)    AS year,
        EXTRACT(week FROM start_time)    AS weekday
    
    FROM events
    WHERE page = 'NextSong';
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#copy_table_queries = [staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
#insert_table_queries = [song_table_insert, artist_table_insert]