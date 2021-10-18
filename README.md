# Sparkify database

## Goal

The goal of this project is to create databases to help the Sparkify's analytis team to understand the songs that their users are listening to.


## Database

Based on songs and log events datasets, it have been created 5 tables to make easyer to query data: songplays, users, songs, artists and time.

Because the analytis team needed a database that provides simple querys and fast aggregations and duplicated data (dernormalized data) and many to many relations is not an issue, Star schema was used to model this database.

### Staging Tables
**Events** is the raw data table, where the access events data are stored. In this table we have access to event_id, artist, auth, firstname, gender, iteminsession, lastname, length, level, location, method,page,registration, sessionid, song, status, ts, useragent and userid data.

**Song_data** is the raw data table where songs data are stored. In this table we have access to num_songs, artist_id, artist_latitude, artist_longitude,artist_location, artist_name, song_id, title,duration and year data.

### Fact Table

**Songplays** is the Fact Table, where the events data are stored along with other ID's to make easyer to query and analyse data. In this table we have access to songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location and user_agent data.

### Dimensions Tables

**Users** table has data about users in the app. In this table we have access to user_id, first_name, last_name, gender and level data.

**Songs** table has data about songs in music database. In this table we have access to song_id, title, artist_id, year and duration data.

**Artists** table has data about artists in music database. In this table we have access to artist_id, name, location, latitude and longitude data.

**Time** table has data about timestamps of records in songplays broken down into specific units. In this table we have access to start_time, hour, day, week, month, year and weekday data.

## How to run:
1. Edit dwh.cfg
2. Execute create_cluster notebook to create a cluster and verify S3 data
3. Execute create_tables.py to drop and create the database and tables.
4. Execute etl.py to store the data into the tables
5. Use test.ipynb notebook to confirm the data insert and do some querys
