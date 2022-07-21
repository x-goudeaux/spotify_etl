# spotify_etl
A small data engineering project. Code that creates a pipeline from a spotify account to an sqlite database.

*Question* What songs has -user- listened to in the past 24 hours?

EXTRACT - Taking all of the desired/ relevant information from Spotify data source
TRANSFORM - Checking for duplicates or null values. Validation function
LOAD - Ensuring the connection to database and loading the data into a schema
(Airflow coming soon)
