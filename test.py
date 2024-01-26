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


# Abre el archivo JSON
with open('cred.json', 'r') as f:
    data = json.load(f)

# Ahora puedes acceder a los datos en el diccionario
print(data["dev"]["DB_USER"])  # Salida: everduvan
