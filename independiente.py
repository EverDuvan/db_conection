import hashlib
import pandas as pd
from sqlalchemy.engine import create_engine
import os
import xlsxwriter
import openpyxl
from tqdm import tqdm
import json
import datetime

def time_df():
    now = datetime.datetime.now()
    data = {'fecha': [now.strftime('%Y-%m-%d')],
            'hora': [now.strftime('%H:%M:%S')]}
    df = pd.DataFrame(data)
    return df

def get_df_from_db(cred: str, db: str, table: str, column: str =[]):
    """cred: json file name, db: db name, table: table name, column: list of columns to select
    return: pandas-df
    """
    with open(cred , 'r') as f:
          data = json.load(f)
    engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format
                           (data[db]["DB_USER"],
                           data[db]["DB_PASS"],
                           data[db]['DB_IP'], 
                           data[db]['DB_PORT'], 
                           data[db]['DB_NAME']))
    query= f'SELECT * from {table}'
    df = pd.read_sql_query(query, engine)
    if column != []:
        df=df[column]
        
# Agregamos la barra de progreso aquí (comentar las 2 líneas siguientes si no se necesita la barra de progreso)
    # for index, row in tqdm(df.iterrows(), total=len(df)):
          # continue
    return df

def get_xlsx_df(table): 
        """table : excel file"""
        if table != '':
                df = pd.read_excel(table, engine='openpyxl', na_filter = False)
        else:
                print('File not found')
                df = pd.DataFrame()
        return df

def df_2_xl(df: pd.DataFrame ,table=''):
        if table == '':
                table = 'file'
        full_path = f'{table}.xlsx'
        with pd.ExcelWriter(full_path,
                        engine='xlsxwriter',
                        engine_kwargs={'options':{'strings_to_urls': False}}) as writer:
              df.to_excel(writer,index=False)
              print ('\nxlsx created !')

def pg_to_excel_chunk(cred: str, db: str, table: str, column: str =[], chunk_size=5000):
        with open(cred, 'r') as f:
              data = json.load(f)
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format
                                (data[db]["DB_USER"],
                                data[db]["DB_PASS"],
                                data[db]['DB_IP'], 
                                data[db]['DB_PORT'], 
                                data[db]['DB_NAME']))
        query= f'SELECT * from {table}'
        full_path = f'{table}.xlsx'
        writer = pd.ExcelWriter(full_path, engine='xlsxwriter', engine_kwargs={'options':{'strings_to_urls': False}})
        # Get the total number of rows in the table for the progress bar
        row_count_query = f'SELECT COUNT(*) from {table}'
        row_count = pd.read_sql_query(row_count_query, engine).values[0][0]
        # Initialize progress bar
        pbar = tqdm(total=row_count)
        # Initialize startrow
        startrow = 0
        # Read and write data in chunks
        for chunk in pd.read_sql_query(query, engine, chunksize=chunk_size):
              if column != []:
                    chunk=chunk[column]
              # Convert datetimes to timezone unaware
              for col in chunk.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
                    chunk[col] = chunk[col].dt.tz_convert(None)
              chunk.to_excel(writer, index=False, startrow=startrow)
              # Update startrow for next chunk
              startrow += chunk_size
              # Update progress bar
              pbar.update(chunk_size)
        # Save Excel file
        writer.close()
        print ('\nxlsx created !')
        # Close progress bar
        pbar.close()


# cred: str, db: str, table: str


def send_df_append(dataframe: pd.DataFrame, cred: str, db: str, table: str):
        with open(cred, 'r') as f:
              data = json.load(f)
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format
                                   (data[db]["DB_USER"],
                                    data[db]["DB_PASS"],
                                    data[db]['DB_IP'], 
                                    data[db]['DB_PORT'], 
                                    data[db]['DB_NAME']))
        dataframe.to_sql(table,
                        engine,
                        schema='public', 
                        if_exists='append', 
                        index=False,
                        chunksize = 100)
        print ('¡Done!')

def send_df_replace(dataframe: pd.DataFrame, cred: str, db: str, table: str):
        with open(cred, 'r') as f:
                data = json.load(f)
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format
                               (data[db]["DB_USER"],
                                data[db]["DB_PASS"],
                                data[db]['DB_IP'], 
                                data[db]['DB_PORT'], 
                                data[db]['DB_NAME']))
        dataframe.to_sql(table,
                         engine,
                         schema='public', 
                         if_exists='replace', 
                         index=False, 
                         chunksize = 100)
        print ('¡Done!')


# print(get_df_from_db('cred.json', 'db_02', 'homologados', ['URL', 'subcategory_id']))
        
# print (get_xlsx_df('filtro.xlsx'))
        
# df_2_xl(time_df(), 'test')

# pg_to_excel_chunk('cred.json', 'db_02', 'homologados', ['URL', 'subcategory_id'], chunk_size=5000)
        
# send_df_append(time_df(), 'cred.json', 'db_02', 'test')

send_df_replace(time_df(), 'cred.json', 'db_02', 'test')