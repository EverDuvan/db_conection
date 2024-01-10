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

def get_df(db: str, table, column=[]):
    """
    [get dataframe from postgres]
    return: pandas-df
    """
    a = eval(os.getenv("DB"))
    db=a[db]
    engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format
                           (db['DB_USER'], 
                            db['DB_PASS'], 
                            db['DB_IP'], 
                            db['DB_PORT'], 
                            db['DB_NAME']))
    query= f'SELECT * from {table}'
    df = pd.read_sql_query(query, engine)
    if column != []:
        df=df[column]
    return df

def get_xlsx_df(table):
      if table != '':
        df = pd.read_excel(table, engine='openpyxl', na_filter = False)
      else:
        print('File not found')
        df = pd.DataFrame()
      return df

if __name__ == '__main__':
    df = get_xlsx_df('filtro.xlsx')
    print (df)

    lista = ['COLOMBIA', '3311']
    #df_filtrado = df[df['PAIS'].isin(lista)]
    df_filtrado = df[df.apply(lambda row: row.astype(str).str.contains('|'.join(lista)).any(), axis=1)]
    print (f'filtrado: \n {df_filtrado}')
