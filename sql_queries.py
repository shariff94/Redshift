import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES
# Redshift default diststyle=AUTO

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                artist TEXT,
                                auth TEXT,
                                first_name TEXT,
                                gender CHAR(1),
                                item_session INT,
                                last_name TEXT,
                                length NUMERIC,
                                level TEXT,
                                location TEXT,
                                method TEXT,
                                page TEXT,
                                registration NUMERIC,
                                session_id INT,
                                song TEXT,
                                status INT,
                                ts BIGINT,
                                user_agent TEXT,
                                user_id INT )""")

staging_songs_table_create =  ("""CREATE  TABLE IF NOT EXISTS staging_songs(
                                num_songs INT,
                                artist_id TEXT,
                                artist_latitude NUMERIC,
                                artist_longitude NUMERIC,
                                artist_location TEXT,
                                artist_name TEXT,
                                song_id TEXT,
                                title TEXT,
                                duration NUMERIC,
                                year INT)""")

songplay_table_create =  ("""CREATE TABLE IF NOT EXISTS songplay(
                            songplay_id INT IDENTITY(1,1) PRIMARY KEY,
                            start_time TIMESTAMP,
                            user_id INT NOT NULL,
                            level TEXT,
                            song_id TEXT,
                            artist_id TEXT,
                            session_id INT,
                            location TEXT,
                            user_agent TEXT)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id INT PRIMARY KEY,
                        first_name TEXT NOT NULL,
                        last_name TEXT NOT NULL,
                        gender CHAR(1),
                        level TEXT)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id TEXT PRIMARY KEY,
                        title TEXT,
                        artist_id TEXT,
                        year INT,
                        duration NUMERIC )""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                          artist_id TEXT PRIMARY KEY,
                          name TEXT,
                          location TEXT,
                          latitude NUMERIC,
                          longitude NUMERIC )""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time_table(
                        start_time TIMESTAMP PRIMARY KEY,
                        hour INT,
                        day INT,
                        week INT,
                        month INT,
                        year INT,
                        weekDay INT  )""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from '{}'
 credentials 'aws_iam_role={}'
 region 'us-west-2' 
 COMPUPDATE OFF STATUPDATE OFF
 JSON '{}'""").format(config.get('S3','LOG_DATA'),
                        config.get('IAM_ROLE', 'ARN'),
                        config.get('S3','LOG_JSONPATH'))

# setting COMPUPDATE, STATUPDATE to speed up COPY

staging_songs_copy = ("""copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    COMPUPDATE OFF STATUPDATE OFF
    JSON 'auto'
    """).format(config.get('S3','SONG_DATA'), 
                config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT  timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time, e.user_id, e.level, 
                                    s.song_id, s.artist_id, e.session_id, e.location, e.user_agent
                            FROM staging_events e, staging_songs s
                            WHERE e.page = 'NextSong' AND
                            e.song =s.title AND
                            e.artist = s.artist_name AND
                            e.length = s.duration""")


user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT distinct  user_id, first_name, last_name, gender, level
                        FROM staging_events
                        WHERE page = 'NextSong'""")


song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT song_id, title, artist_id, year, duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          SELECT distinct artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
                          FROM staging_songs
                          WHERE artist_id IS NOT NULL""")

time_table_insert = ("""INSERT INTO time_table(start_time, hour, day, week, month, year, weekDay)
                        SELECT start_time, extract(hour from start_time), extract(day from start_time),
                                extract(week from start_time), extract(month from start_time),
                                extract(year from start_time), extract(dayofweek from start_time)
                        FROM songplay""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]