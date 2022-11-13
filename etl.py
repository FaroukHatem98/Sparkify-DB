import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """processing the song files and getting data from it insert in songs and artists tables"""
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = [df.song_id[0], df.title[0], df.artist_id[0], int(df.year[0]), df.duration[0] ]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_df = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = list(artist_df.iloc[0])
    cur.execute(artist_table_insert, artist_data)
    

def process_log_file(cur, filepath):
    """processing the log files and getting data from it to insert in time, users and songplays tables"""
    # open log file
    df = pd.read_json(filepath, lines=True)
    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['TS'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_s = df['TS']
    time_df = pd.DataFrame(time_s, columns= ['TS'])
    time_df['Hour'] = time_df['TS'].dt.hour
    time_df['Day'] = time_df['TS'].dt.day
    time_df['WeekOfYear'] = time_df['TS'].dt.week
    time_df['Month'] = time_df['TS'].dt.month
    time_df['Year'] = time_df['TS'].dt.year
    time_df['WeekDay'] = time_df['TS'].dt.dayofweek

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        s = row.song.replace('\'', '\'\'')
        a = row.artist.replace('\'', '\'\'')
        so = song_select.format(row.length, s, a)
        cur.execute(so)
        results = cur.fetchone()
        if results:
            songid, artistid = results
              
        else:
            songid, artistid = None , None
        
        songplay_data = (row.TS, row.userId, row.level, songid, 
                             artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
            


def process_data(cur, conn, filepath, func):
    """getting all the json fills from the fillpath and aplly funcs on every fill to fill the tables """
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