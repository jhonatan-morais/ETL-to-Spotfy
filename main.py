import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "lfnicolao"  #Spotify username
TOKEN = ""  #token Spotify API



def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Checa se dataframe está vazio
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

        # Checagem da Primary Key
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Chegagem de itens com valores vazios
    if df.isnull().values.any():
        raise Exception("Null values found")

if __name__ == "__main__":

    # Parte de Extração do processo ETL

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    # Conversão de time para Unix timestamp em milissegundos
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=60) # definição de quantos dias (para trás) será buscado pela API
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download das musicas conforme parâmetro "days" da linha 44
    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp),
        headers=headers)

    data = r.json()
    # Definição dos campos de interesse
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extração dos itens do objeto json
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Criação de um dicionário para ser introduzido ao dataframe pandas - Transform do processo ETL
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])

    # Validação
    if check_if_valid_data(song_df):
        print("Data valid, proceed to Load stage")

    # Load e fim do processo ETL

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
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")
