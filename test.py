import json
import pandas as pd
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv
import os
load_dotenv()
import datetime

def time_df():
    now = datetime.datetime.now()
    data = {'fecha': [now.strftime('%Y-%m-%d')],
            'hora': [now.strftime('%H:%M:%S')]}
    df = pd.DataFrame(data)
    return df

def send_df_replace_mysql(df, cred: str, db, table):
    with open(cred , 'r') as f:
          data = json.load(f)
          engine = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'.format
                                 (data[db]["DB_USER"],
                                  data[db]["DB_PASS"],
                                  data[db]['DB_IP'],
                                  data[db]['DB_NAME']))
          df.to_sql(con=engine, name=table, if_exists='replace')
          print ('¡Done!')

df = time_df()
print (df)
send_df_replace_mysql(df, 'cred.json', 'db_03', 'test')