import hashlib
import pandas as pd
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv
import os
import xlsxwriter
import openpyxl
from tqdm import tqdm
load_dotenv()

class PGToDf:
    """
    [get dataframe from postgres]
    """
    def __init__(self, table='', credentials='', column=[]):
    
        self._credentials = credentials
        self._table = table
        self._column = column
        
    def __str__(self):
        return f'\ndatabase: {self._credentials}\ndb table: {self._table}\ntable columns: {self._column}'
    
    @property
    def get_df(self):
        """
        [get dataframe from postgres]
        return: pandas-df
        """
        a = eval(os.getenv("basesdedatos"))
        credentials=a[self._credentials]
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format
                               (credentials['DB_USER'], 
                                credentials['DB_PASS'], 
                                credentials['DB_IP'], 
                                credentials['DB_PORT'], 
                                credentials['DB_NAME']))
        query= f'SELECT * from {self._table}'
        df = pd.read_sql_query(query, engine)
        if self._column != []:
            df=df[self._column]
        return df

    @property
    def get_xlsx_df(self):
        """
        [get dataframe from xlsx]
        return: pandas-df
        """
        if self._table != '':
            df = pd.read_excel(self._table, engine='openpyxl', na_filter = False)
        else:
            print('File not found')
            df = pd.DataFrame()
        return df

    @staticmethod
    def df_2_xl(df,table=''):
        """
        [save dataframe to xlsx]
        parameter
        ---------
        df: pandas-df
            A pandas df whith exact table and columns in postgres 
        table: str
            table name in postgres 
        """
        if table == '':
            table = 'file'
        full_path = f'{table}.xlsx'
        with pd.ExcelWriter(full_path,
                            engine='xlsxwriter',
                            engine_kwargs={'options':{'strings_to_urls': False}}) as writer:
            df.to_excel(writer,index=False)
            print ('\nxlsx created !')

    @property
    def pg_to_excel_chunk(self, table='', chunk_size=5000):
        """
        [get dataframe from postgres and save to xlsx]
        parameter
        ---------
        table: str
            table name in postgres 
        chunk_size: int
            The number of rows per chunk
        """
        # Get database credentials
        a = eval(os.getenv("basesdedatos"))
        credentials=a[self._credentials]
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format
                            (credentials['DB_USER'], 
                                credentials['DB_PASS'], 
                                credentials['DB_IP'], 
                                credentials['DB_PORT'], 
                                credentials['DB_NAME']))
        query= f'SELECT * from {self._table}'

        # Prepare Excel writer
        if table == '':
            table = 'file'
        full_path = f'{table}.xlsx'
        writer = pd.ExcelWriter(full_path, engine='xlsxwriter', engine_kwargs={'options':{'strings_to_urls': False}})

        # Get the total number of rows in the table for the progress bar
        row_count_query = f'SELECT COUNT(*) from {self._table}'
        row_count = pd.read_sql_query(row_count_query, engine).values[0][0]

        # Initialize progress bar
        pbar = tqdm(total=row_count)

        # Initialize startrow
        startrow = 0

        # Read and write data in chunks
        for chunk in pd.read_sql_query(query, engine, chunksize=chunk_size):
            if self._column != []:
                chunk=chunk[self._column]
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

    @property
    def get_xlsx_df(self):
        """
        [get dataframe from xlsx]
        return: pandas-df
        """
        if self._table != '':
            df = pd.read_excel(self._table, engine='openpyxl', na_filter = False)
        else:
            print('File not found')
            df = pd.DataFrame()
        return df

 
# ************************** DfToPg *********************************


class DfToPG(PGToDf):
    """
    [save dataframe to postgres]
    """
    def __init__(self, dataframe, table, credentials=''):
        super().__init__(table, credentials)

        self._dataframe = dataframe

    @property
    def send_df_append(self):
        if self._credentials or self._table != '':
            a = eval(os.getenv("basesdedatos"))
            credentials=a[self._credentials]
            engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format
                                   (credentials['DB_USER'], 
                                    credentials['DB_PASS'], 
                                    credentials['DB_IP'], 
                                    credentials['DB_PORT'], 
                                    credentials['DB_NAME']))
            self._dataframe.to_sql(self._table, 
                                   engine, 
                                   schema='public', 
                                   if_exists='append', 
                                   index=False, 
                                   chunksize = 100)
            print ('¡Done!')

    @property
    def send_df_replace(self):
        if self._credentials or self._table != '':
            a = eval(os.getenv("basesdedatos"))
            credentials=a[self._credentials]
            engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format
                                   (credentials['DB_USER'], 
                                    credentials['DB_PASS'], 
                                    credentials['DB_IP'], 
                                    credentials['DB_PORT'], 
                                    credentials['DB_NAME']))
            self._dataframe.to_sql(self._table, 
                                   engine, 
                                   schema='public', 
                                   if_exists='replace', 
                                   index=False, 
                                   chunksize = 100)
            print ('¡Done!')

    @property
    def send_df_replace_mysql(self):
        if self._credentials or self._table != '':
            a = eval(os.getenv("basesdedatos"))
            credentials=a[self._credentials]
            engine = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'.format
                                   (credentials['DB_USER'], 
                                    credentials['DB_PASS'], 
                                    credentials['DB_IP'], 
                                    credentials['DB_NAME']))
            #engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(credentials['DB_USER'], credentials['DB_PASS'], credentials['DB_IP'], credentials['DB_PORT'], credentials['DB_NAME']))
            self._dataframe.to_sql(con=engine, name=self._table, if_exists='replace')
            #self._dataframe.to_sql(self._table, engine, schema='public', if_exists='replace', index=False)
            print ('¡Done!')
