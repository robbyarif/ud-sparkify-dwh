import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN=config['IAM_ROLE']['ARN']
LOG_DATA=config['S3']['LOG_DATA']
LOG_JSONPATH=config['S3']['LOG_JSONPATH']
SONG_DATA=config['S3']['LOG_DATA']
S3_REGION=config['S3']['S3_REGION']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length DECIMAL(10,5),
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration DECIMAL(18,1),
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude REAL, 
    artist_longitude REAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DECIMAL(10,5),
    year SMALLINT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY ,
    start_time timestamp NOT NULL SORTKEY,
    user_id int NOT NULL,
    level text,
    song_id text,
    artist_id text DISTKEY,
    session_id int,
    location text,
    user_agent text
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY SORTKEY,
    first_name text,
    last_name text,
    gender text,
    level text)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY SORTKEY,
    title text,
    artist_id text,
    year int,
    duration DECIMAL(10,5))
DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar DISTKEY SORTKEY,
    name text,
    location text,
    latitude real,
    longitude real);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY SORTKEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int)
DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
timeformat as 'epochmillisecs'
json {} region {}
truncatecolumns blanksasnull emptyasnull;
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH, S3_REGION)

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
json 'auto' region {}
truncatecolumns blanksasnull emptyasnull;
""").format(SONG_DATA, DWH_ROLE_ARN, S3_REGION)


# FINAL TABLES

# NOTE: songplays JOIN is not strict, meaning it will do lookup for the songs with same song title and duration while   
# doing lookup on artist with same artist name separately. Leaving the song or artist match decision to user. 
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT evt.ts, evt.userId, evt.level, s.song_id, a.artist_id, evt.sessionId, evt.location, evt.userAgent
FROM staging_events as evt
LEFT JOIN songs as s ON s.title=evt.song AND s.duration=evt.length
LEFT JOIN artists as a ON a.name=evt.artist
WHERE 
    evt.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT evt1.userId, evt1.firstName, evt1.lastName, evt1.gender, evt1.level
FROM staging_events as evt1
WHERE 
    evt1.page = 'NextSong' AND evt1.userId IS NOT NULL
    AND userId NOT IN (SELECT DISTINCT user_id FROM users)
    AND ts = (SELECT max(ts) FROM staging_events evt2 
              WHERE evt2.userId = evt1.userId AND evt2.page='NextSong')
ORDER BY userId ASC;
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE
    song_id IS NOT NULL
    AND song_id NOT IN (SELECT DISTINCT song_id FROM songs) ;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE
    artist_id IS NOT NULL
    AND artist_id NOT IN (SELECT DISTINCT artist_id from artists);
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT 
    ts as start_time, 
    datepart(hour, ts) as hour, 
    datepart(day, ts)as day, 
    datepart(week,ts) as week_of_year, 
    datepart(month, ts) as month,
    datepart(year, ts) as year, 
    datepart(dow, ts) as weekday
FROM staging_events
WHERE
    ts IS NOT NULL AND page='NextSong' 
    AND ts NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
