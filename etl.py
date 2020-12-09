import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process the song json file, indicated by the filepath, inserting the data into the tables 'songs', and 'artists'.
    """
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = song_data = list(df[['song_id','title','artist_id', 'year', 'duration']].iloc[0].values)
    song_data[3] = int(song_data[3])
    song_data[4] = float(song_data[4])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =  list(df[['artist_id','artist_name', 'artist_location','artist_latitude', 'artist_longitude']].iloc[0].values)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process the log file in json format, indicated by the filepath, following the steps below:
    - Open the file and filter the data containing the action 'NextSong'
    - Retrieve the timestamp information, extract other meaningful metrics from the date, and insert it in the 'time' table.
    - Retrieve the user information and insert into the 'users' table.
    - The log file does not contain the artist and song Ids, for that we need to first retrive this information from the artists and songs tables,
    using a select query implemented in the 'sql_queries.py' file.
    - After joining the missing information, the script then insert the records in the 'songplays' table
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    df.ts = pd.to_datetime(df.ts, unit='ms')
    t = df.ts
    
    # insert time data records
    time_data = (t,t.dt.hour, t.dt.day,t.dt.week,t.dt.month,t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName', 'lastName', 'gender','level']]
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
    This script receives the cursor, the connection to the database, the path to the data directory and the function for processing the data.
    - The first section of the script gets the filepath of all the files within a directory
    - The second section runs the processing function for each file, passing the cursor as argument.
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