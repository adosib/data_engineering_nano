# What
This project is a part of the Udacity Data Engineering Nanodegree program. In this project, I create tables within a datamart that are linked in a star schema. I then develop an ETL process that normalizes messy JSON log data and populates the tables. Read more about the details below in the 'Background' section.

## Highlights and things learned
1. The data includes some non-valid JSON data which I parse and structure into a standard JSON format.
2. I learned how to walk a directory structure to process all the files contained therein.
3. I learned to zip() two lists into a Pandas dataframe.
4. I learned to use the Postgres ORM pycopg2 library

## Background
### Project
A startup called Sparkify wants to analyze the data they've been collecting on songs 
and user activity on their new music streaming app. 
The analytics team is particularly interested in understanding what songs users 
are listening to. Currently, they don't have an easy way to query their data, 
which resides in a directory of JSON logs on user activity on the app, 
as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed 
to optimize queries on song play analysis, and bring you on the project. 
Your role is to create a database schema and ETL pipeline for this analysis. 
You'll be able to test your database and ETL pipeline by running queries given 
to you by the analytics team from Sparkify and compare your results with 
their expected results.

### Database design
The database is designed in a star schema with a fact table at the center (songplays) and 4 dimensional tables - users, songs, artists, time.
This design is ideal for querying data and doing aggregations on that data. The database is in first normal form, but could be brought to second
normal form if songs did not include artist_id (there are multiple artists per song); one way of possibly implementing this is using a snowflake 
schema to build more dimension tables (say one for group/band) and restructing the songs table. However, more dimensions requires more joins which 
reduces read speeds on the database.

## Example queries
#### Say we want to know which song has the most artists
SELECT song_id, COUNT(artist_id) num_artists FROM songs GROUP BY song_id ORDER BY num_artists
#### What are the top songs being played by region?
SELECT sp.location, s.title, COUNT(sp.song_id) play_count 
FROM (SELECT DISTINCT user_id, location, song_id FROM songplay) sp JOIN songs s ON s.song_id = sp.song_id
GROUP BY sp.location, s.title
ORDER BY play_count