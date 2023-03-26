import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes song data files in JSON format by Pandas DataFrame and inserts data into dimension tables.

    Args:
        (psycopg2.extensions.cursor) cur - cursor object using the connection created by psycopg2
        (str) filepath - directory path of song data files
    Returns:
        None (data processed in Pandas DataFrame and inserted into Postgres tables directly)
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = [df['song_id'][0], df['title'][0], df['artist_id'][0], int(df['year'][0]), df['duration'][0]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df['artist_id'][0], df['artist_name'][0], df['artist_location'][0], df['artist_latitude'][0], df['artist_longitude'][0]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes log data files in JSON format by Pandas DataFrame and inserts data into dimension tables. Data entries going to songplays, the fact table, are also prepared here.

    Args:
        (psycopg2.extensions.cursor) cur - cursor object using the connection created by psycopg2
        (str) filepath - directory path of log data files
    Returns:
        None (data processed in Pandas DataFrame and inserted into Postgres tables directly)
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df['ts']
    
    # insert time data records
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_data = {
    column_labels[0] : t,
    column_labels[1] : pd.to_datetime(t, unit='ms').dt.hour,
    column_labels[2] : pd.to_datetime(t, unit='ms').dt.day,
    column_labels[3] : pd.to_datetime(t, unit='ms').dt.week,
    column_labels[4] : pd.to_datetime(t, unit='ms').dt.month,
    column_labels[5] : pd.to_datetime(t, unit='ms').dt.year,
    column_labels[6] : pd.to_datetime(t, unit='ms').dt.weekday
    }
    time_df = pd.DataFrame(time_data)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame({
    'user_id' : df['userId'],
    'first_name' : df['firstName'],
    'last_name' : df['lastName'],
    'gender' : df['gender'],
    'level' : df['level']
    })

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Wrapper function created to call process_song_file() and process_log_file(), two functions produced previously to fulfill tasks to process JSON data files and insert data into Postgres tables.

    Args:
        (psycopg2.extensions.cursor) cur - cursor object using the connection created by psycopg2
        (psycopg2.extensions.connection) conn - connection handle created by psycopg2
        (str) filepath - directory path of log data files
        (str) func - name of Python function to be called by this subroutine
    Returns:
        None
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()