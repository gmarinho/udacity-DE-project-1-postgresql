# Udacity Data Engineering Project 1 - Data Modeling with PostgreSQL

This project implements the database modeling of a music streaming app data from a startup called 'Sparkfy'. Sparkfy is collecting user activity data from their new app, but it was first stored as JSON files containing log activity, user information, songs and artists.

The purpose of this project is to create a simple logical modeling of the database using Postgres, implementing the ETL pipeline to process and ingest the data in the new database. To help Sparkify data analysts to get up to speed with this new database, some queries where previously designed and you can check them out in the section *Useful Queries*.

## Installation and dependencies

In order to run this project codes you will need the folowing Python packages:
* os
* pandas
* glob
* psycopg2

Use the package manager *pip* to install the dependencies listed above.

## File Descriptions

* **sql_queries.py** - This file contains the implemented queries to drop and create tables, perform the data ingestion, and an intermediary query for the ETL process.
* **create_tables.py** - This file implements the setup of the database using queries implemented in the file sql_queries.py. It drops, if exists, the existing database and tables, (re)create them, and closes the connection to the database.
* **etl.py** - The file implements the full logic to read all the input files from the data directory, and inserts the processed information into the database. It populates the tables 'songs', 'artists','users','time', and 'songplays'
* **data/** - This folder structure contains the data provided by the 'Sparkfy' analysts to be ingested in the database.


## Database Schema
The modeled database contains 4 dimension tables and a unique fact table in star shape. The tables and the description of the stored data are described below:
* Users: User information such as full name, gender, and subscription type (paid or free)
* Songs: List of songs within the music streaming app, containing the song title, the artist id, the year of release and the song duration
* Artists: Contains the artists information such as name, location, latitude and longitude
* Time: Registers the timestamp for the users action to play a song, it contains the time stamp, and the other metadata related to the timestamp, such as day, hour, month, year, weekday, and week number of the year.
* Songplays: Registers the actual user activity, such as start time, user id, song id, subscription type, artist id, session id, location and the agent used to stream the music.

The diagram below shows the database representation with its tables and attributes:
[Alt text](/udacity_postgress_project_1.png?raw=true "Title")


## How to run this project
In order to run this project open a Python 3+ console, and use de commands listed below to complete the whole process:
```python
%run create_tables.py
%run etl.py
```

## Useful Queries
Below you have some queries to retrieve data from the database:
1. Get the average number of musics listened per session by users according to their location and subscription type:
```SQL
SELECT mid_table.location, mid_table.level, AVG(mid_table.musics_played) AS average_music_count FROM
(SELECT user_id, session_id, location, level, COUNT(songplay_id) AS musics_played
FROM songplays
GROUP BY user_id, session_id, location, level) AS mid_table
GROUP BY mid_table.location, mid_table.level
ORDER BY mid_table.location, mid_table.level
```
2. Get the all time hottest 50 musics of a region
```SQL
SELECT songs.title as "Song Title", artists.name as "Artist", songs.duration, songplays.location, COUNT(songplay_id) AS play_count
FROM (songs JOIN artists ON songs.artist_id = artists.artist_id) JOIN songplays ON songs.song_id = songplays.song_id
GROUP BY songs.title, artists.name, songs.duration, songplays.location
ORDER BY play_count
LIMIT 50
```
