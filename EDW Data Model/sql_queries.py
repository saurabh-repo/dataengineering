import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
[LOG_DATA, LOG_JSONPATH, SONG_DATA] = config['S3'].values()
ARN=config['IAM_ROLE']['ARN']
# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"
songplay_table_drop = "drop table if exists songplays"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events ( artist varchar,
auth varchar,
firstname varchar,
gender char(1),
iteminsession bigint,
lastname varchar,
length decimal,
level char(4),
location varchar,
method char(6),
page varchar,
registration decimal,
sessionid bigint,
song varchar,
status smallint,
ts bigint,
useragent varchar,
userid bigint
)

""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs ( num_songs int,
artist_id varchar,
artist_latitude DECIMAL,
artist_longitude DECIMAL,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration decimal,
year smallint
)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS  users ( 
	user_id bigint NOT NULL PRIMARY KEY sortkey distkey, 
	first_name varchar NOT NULL, 
	last_name varchar NOT NULL, 
	gender character(1) , 
	level character(4) NOT NULL
) 
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS  songs ( 
	song_id varchar NOT NULL PRIMARY KEY sortkey distkey, 
	title varchar NOT NULL, 
	artist_id varchar NOT NULL, 
	year smallint , 
	duration numeric 
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS  artists ( 
	artist_id varchar PRIMARY KEY sortkey distkey, 
	name varchar , 
	location varchar , 
	latitude numeric , 
	longitude numeric 
) 
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS  time ( 
	start_time bigint NOT NULL PRIMARY KEY, 
	hour smallint NOT NULL, 
	day smallint NOT NULL, 
	week smallint NOT NULL, 
	month smallint NOT NULL, 
	year smallint NOT NULL, 
	weekday smallint NOT NULL
) diststyle all
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS  songplays ( 
    songplay_id bigint IDENTITY(0,1) ,
	start_time bigint REFERENCES time(start_time), 
	user_id bigint REFERENCES users(user_id) sortkey distkey, 
	level char(4) NOT NULL, 
	song_id varchar REFERENCES songs(song_id),
	artist_id varchar REFERENCES artists(artist_id), 
	session_id bigint NOT NULL, 
	location varchar NOT NULL, 
	user_agent varchar NOT NULL
)
""")

# STAGING TABLES
#the copy commandcodehad been referenced from the previous knowledge center question-answers.

staging_events_copy = (
    """
    COPY staging_events
    FROM {} 
    iam_role '{}'
    FORMAT AS JSON {}
    REGION 'us-west-2'
    COMPUPDATE ON
    STATUPDATE ON;
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = (
    """
    COPY staging_songs
    FROM {} 
    iam_role '{}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto'
    COMPUPDATE ON
    STATUPDATE ON;
""").format(SONG_DATA, ARN)

# FINAL TABLES

user_table_insert = ("""insert into users (select userid,firstname,lastname,gender,level from staging_events where userid IS NOT NULL)
""")

song_table_insert = ("""insert into songs 
(select song_id,title,artist_id,year,duration from staging_songs where artist_id IS NOT NULL)
""")

artist_table_insert = ("""insert into artists
(select artist_id,artist_name,artist_location,artist_latitude,artist_longitude from staging_songs where artist_id IS NOT NULL)
""")

#Insert statement reused from Knowledge center
time_table_insert = ("""
INSERT INTO time
Select ts
,EXTRACT(HOUR FROM t_start_time) As t_hour
,EXTRACT(DAY FROM t_start_time) As t_day
,EXTRACT(WEEK FROM t_start_time) As t_week
,EXTRACT(MONTH FROM t_start_time) As t_month
,EXTRACT(YEAR FROM t_start_time) As t_year
,EXTRACT(DOW FROM t_start_time) As t_weekday
FROM (
SELECT distinct ts as ts,'1970-01-01'::date + ts/1000 * interval '1 second' as t_start_time
FROM staging_events
) 
""")

songplay_table_insert = ("""insert into songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
(
select 
 staging_events.ts
,staging_events.userid
,staging_events.level
,staging_songs.song_id
,staging_songs.artist_id
,staging_events.sessionid
,staging_events.location
,staging_events.useragent
from staging_events JOIN staging_songs
ON staging_events.artist = staging_songs.artist_name
AND staging_events.song = staging_songs.title
AND staging_events.length = staging_songs.duration
WHERE
staging_events.userid IS NOT NULL
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
