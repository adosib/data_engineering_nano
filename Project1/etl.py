import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json
from datetime import datetime

def valid_json(filepath):
    try:
        with open(filepath, 'r') as json_data:
            valid_json = json.load(json_data)
            
        df = pd.DataFrame([valid_json])
        
    except json.decoder.JSONDecodeError:
        with open(filepath, 'r') as data:
            contents = data.read()
        # Turn the contents string into a list of char elements
        contents = list(contents)
        
        # Enclose the to-be valid JSON string with brackets
        contents.insert(0, '[') # insert opening bracket at the beginning
        contents.append(']') # append a closing bracket to the end
        
        # Identify the index of every closing brace
        index_closing_brace = [i for i in range(len(contents)) if contents[i] == '}']
        # Index of the last closing brace
        last_brace = index_closing_brace[-1]
        
        for i, brace_index in enumerate(index_closing_brace):
            if brace_index != last_brace:
                # If we're not at the closing brace, insert a comma after the index of the brace.
                # Since the index shifts every time we insert a comma, we have to add i.
                contents.insert(brace_index+i+1, ',')
        
        valid_json = ''.join([str(elem) for elem in contents])
        
        df = pd.read_json(valid_json)
    
    return df


def process_song_file(cur, filepath):
    # open song file
    df = valid_json(filepath)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = valid_json(filepath)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    ms = df['ts']
    t = ms.apply(lambda x: datetime.utcfromtimestamp(x//1000).replace(microsecond = x%1000*1000))
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.day_name()]
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday_name']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    # Reset the indices on the dataframes
    df.reset_index(drop = True, inplace = True)
    time_df.reset_index(drop = True, inplace = True)

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
        songplay_data = [time_df.iloc[index].timestamp, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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