import json
import pandas as pd
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv
import os
load_dotenv()
import datetime

now = datetime.datetime.now()
data = {'fecha': [now.strftime('%Y-%m-%d')],
        'hora': [now.strftime('%H:%M:%S')]}
df = pd.DataFrame(data)
print(df)

def time_df():
    now = datetime.datetime.now()
    data = {'fecha': [now.strftime('%Y-%m-%d')],
            'hora': [now.strftime('%H:%M:%S')]}
    df = pd.DataFrame(data)
    return df


print(time_df())



"""
with open('cred.json', 'r') as f:
    data = json.load(f)
# Ahora puedes acceder a los datos en el diccionario
print(data["db_01"]["DB_USER"],
            data["db_01"]['DB_PASS'],
            data["db_01"]['DB_IP'], 
            data["db_01"]['DB_PORT'], 
            data["db_01"]['DB_NAME'])
"""