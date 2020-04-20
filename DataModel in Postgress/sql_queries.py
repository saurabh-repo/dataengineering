# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS  songplays ( 
    songplay_id serial ,
	start_time bigint REFERENCES time(start_time), 
	user_id bigint REFERENCES users(user_id), 
	level char(4) NOT NULL, 
	song_id varchar REFERENCES songs(song_id),
	artist_id varchar REFERENCES artists(artist_id), 
	session_id bigint NOT NULL, 
	location varchar NOT NULL, 
	user_agent varchar NOT NULL, 
	CONSTRAINT songplays_pk PRIMARY KEY (songplay_id)
)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS  users ( 
	user_id bigint NOT NULL, 
	first_name varchar NOT NULL, 
	last_name varchar NOT NULL, 
	gender character(1) , 
	level character(4) NOT NULL, 
	CONSTRAINT users_pk PRIMARY KEY (user_id) 
)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS  songs ( 
	song_id varchar NOT NULL, 
	title varchar NOT NULL, 
	artist_id varchar NOT NULL, 
	year smallint , 
	duration numeric NOT NULL, 
	CONSTRAINT songs_pk PRIMARY KEY (song_id) 
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS  artists ( 
	artist_id varchar , 
	name varchar , 
	location varchar , 
	latitude numeric , 
	longitude numeric , 
	CONSTRAINT artists_pk PRIMARY KEY (artist_id) 
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS  time ( 
	start_time bigint NOT NULL, 
	hour smallint NOT NULL, 
	day smallint NOT NULL, 
	week smallint NOT NULL, 
	month smallint NOT NULL, 
	year smallint NOT NULL, 
	weekday smallint NOT NULL, 
	CONSTRAINT time_pk PRIMARY KEY (start_time) 
)
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO SONGPLAYS (start_time,user_id
,level,song_id,artist_id,session_id,location,user_agent)
VALUES (%s, %s, %s, %s, %s,%s,%s,%s)
""")

user_table_insert = ("""INSERT INTO USERS (user_id,first_name,last_name,gender,level)
VALUES (%s, %s, %s, %s, %s) on conflict (user_id) do update set level=excluded.level
""")

song_table_insert = ("""INSERT INTO SONGS (song_id,title,artist_id,year,duration) 
VALUES (%s, %s, %s, %s, %s) on conflict (song_id) do nothing
""")

artist_table_insert = ("""INSERT INTO artists (artist_id,name,location,latitude,longitude)
VALUES (%s, %s, %s, %s, %s) on conflict (artist_id) do nothing
""")


time_table_insert = ("""INSERT INTO TIME (start_time,hour,day,week,month,year,weekday)
VALUES(%s, %s, %s, %s, %s, %s, %s) on conflict (start_time) do nothing
""")

# FIND SONGS

song_select = ("""SELECT s.song_id, s.artist_id from SONGS S JOIN ARTISTS A ON S.artist_id = A.artist_id
where s.title = %s and a.name = %s and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]