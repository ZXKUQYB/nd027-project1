# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# If you are using PostgreSQL version 10+, check this blog post before creating a table with SERIAL data type.
# https://www.2ndquadrant.com/en/blog/postgresql-10-identity-columns/

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL, start_time timestamp, user_id int, level char(4) NOT NULL, song_id varchar, artist_id varchar, session_id int NOT NULL, location varchar, user_agent varchar, CONSTRAINT pk_songplays PRIMARY KEY(songplay_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int, first_name varchar NOT NULL, last_name varchar NOT NULL, gender char NOT NULL, level char(4) NOT NULL, CONSTRAINT pk_users PRIMARY KEY(user_id));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar, title varchar NOT NULL, artist_id varchar NOT NULL, year int, duration numeric(8,5), CONSTRAINT pk_songs PRIMARY KEY(song_id));
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar, name varchar NOT NULL, location varchar, latitude numeric(8,5), longitude numeric(8,5), CONSTRAINT pk_artists PRIMARY KEY(artist_id));
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time timestamp, hour int, day int, week int, month int, year int, weekday int, CONSTRAINT pk_time PRIMARY KEY(start_time));
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (to_timestamp(%s::double precision/1000), %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT pk_songs DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT pk_artists DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (to_timestamp(%s::double precision/1000), %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT pk_time DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id FROM songs s INNER JOIN artists a ON s.artist_id = a.artist_id WHERE s.title = %s AND a.name = %s AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]