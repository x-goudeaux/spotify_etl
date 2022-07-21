import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

#Spotify username
USER_ID = "x_goudeaux" 

#Spotify Token (generate from API site and create Spotify account for free)
#https://developer.spotify.com/console/get-recently-played/?limit=3&after=&before=
TOKEN = "BQAWpMxg6LXdNWpEmiAUT3SqDdE5z13cCV21I1xGdokUfQakV_fJvNYqoCbL0EL0zP5MnQL7QSpzU9dlGE7a--yLg0elIa_Vtsxrz47pWJgBb6Ma87pWI6nBA3IXSPTdfVRPMj3xDVEwlwUYSgkkDLpgwRbx9ak4bFl41lEzmLN5hy4Ywm6a8ofoypDS5icj_Mii6kNE"


#Transform check; cleanup before loading to database
def check_if_valid_data( df: pd.DataFrame) -> bool:
    #check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    #Primary Key check (primary key == played_at)
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    #Check for NULL values
    if df.isnull().values.any():
        raise Exception("NULL values found")

    #Check that data is from last 24 hours i.e. yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp,"%Y-%m-%d") < yesterday:
            raise Exception("At least one of the returned songs does not come from the last 24 hours")

    return True

    
  

if __name__ == "__main__":

    #Extract part of ETL process
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    #Convert time to Unix timestamp in miliseconds
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    #Download your songs listened to "after yesterday" i.e. last 24 hours
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    #print(data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    #Extracting only the relevant information from json object
    try:
        for song in data["items"]:
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            played_at_list.append(song["played_at"])
            timestamps.append(song["played_at"][0:10])
    except:
        print("Error. Generate new Access Token")
        
    #Dictionary for dataframe transformation
    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name" , "artist_name" , "played_at", "timestamp"])

    #Validation to Load
    if check_if_valid_data(song_df):
        print("Data is valid. Proceed to Load stage.")

        #Load
        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        conn = sqlite3.connect('my_played_tracks.sqlite')
        cursor = conn.cursor()



        sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)

        )
        """

        cursor.execute(sql_query)

        try:
            song_df.to_sql("my_played_tracks" , engine, index=False, if_exists='append')

        except:
            print("Data already exists in the database")

        conn.close()
        print("Closed database succsessfully")
