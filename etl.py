import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an argument.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur  - The cursor variable
    * filepath - The file path to the song file

    Returns nothing.
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_data = df[song_data_columns].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df[artist_columns].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an argument.
    It extracts the basic songplay information (timestamp, song, artist and length). 
    Aditional time information (year, month, weekday, hour, day, week) is extracted 
    and insert into time table.

    Then it extracts the song_id and artist_id. Finally, songplay data (timestamp, userId, 
    level, songid, artistid, sessionId, location, userAgent) is insert into songplay table.

    INPUTS: 
    * cur  - The cursor variable
    * filepath - The file path to the song file

    Returns nothing.
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df[['ts']].copy()
    t['start_time'] = t['ts'].apply(lambda x: pd.Timestamp(x, unit='ms'))
    
    # insert time data records
    t['year'] = t['start_time'].dt.year
    t['month'] = t['start_time'].dt.month
    t['weekday'] = t['start_time'].dt.weekday
    t['hour'] = t['start_time'].dt.hour
    t['day'] = t['start_time'].dt.day
    t['week'] = t['start_time'].dt.week
    column_labels_time = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = t[column_labels_time]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_columns]

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

        timestamp = pd.Timestamp(row.ts, unit='ms')

        # insert songplay record
        songplay_data = (timestamp, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This procedure processes read all files of the filepath input. Then, a loop is runned and 
    over each file and the respective function are used to insert data into tables.

    INPUTS: 
    * cur  - The cursor variable
    * conn - The connection variable
    * filepath - The file path to the song file
    * func - Function that will be executed over files

    Returns nothing.
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
    """
    This function create a database connection, create a cursor and execute the ETL process.
    """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
