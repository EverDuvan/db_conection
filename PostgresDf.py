import hashlib
import pandas as pd
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv
import os
import xlsxwriter
import openpyxl
load_dotenv()
                     

class PGToDf:
    """ [Download data from postgres to dataframe]

    parameters
    ----------
    credentials: str 
        credentials must to be a dict of dicts in .env archive whith all the credential to conect db
    table: str 
        name of table of df 
    column: list
        A list o columns of the df, can be blank to obtain all the columns 

    Returns
    ---------
        [dataframe] 
            pandas df of a table in postgres
    """    

    def __init__(self, table, credentials='', column=[]):

        self._credentials = credentials
        self._table = table
        self._column = column
        
    def __str__(self) -> str:
        return f'\ndatabase: {self._credentials}\ndb table: {self._table}\ntable columns: {self._column}'
    
    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, table):
        self._table = table

    @property
    def credentials(self):
        return self._credentials

    @credentials.setter
    def credentials(self, credentials):
        self._credentials = credentials

    @property
    def column(self):
        return self._column

    @column.setter
    def column(self, column):
        self._column = column      

    @property
    def get_df(self):
        a = eval(os.getenv("basesdedatos"))
        credentials=a[self._credentials]
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(credentials['DB_USER'], credentials['DB_PASS'], credentials['DB_IP'], credentials['DB_PORT'], credentials['DB_NAME']))
        query= f'SELECT * from {self._table}'
        df = pd.read_sql_query(query, engine)
        if self._column != []:
            df=df[self._column]
        return df

    @property
    def get_xlsx_df(self):
        if self._table != '':
            df = pd.read_excel(self._table, engine='openpyxl', na_filter = False)
        else:
            print('File not found')
            df = pd.DataFrame()
        return df

           
    @property
    def get_secret(secret_file: str) -> str:
        path = f'/run/secrets/{secret_file}'
        with open(path) as f:
            secret = f.readline().strip()
        return secret

    @staticmethod
    def df_2_xl(df,table):
        if table == '':
            table = 'file'
        full_path = f'{table}.xlsx'
        with pd.ExcelWriter(full_path,
                            engine='xlsxwriter',
                            options={'strings_to_urls': False}) as writer:
            df.to_excel(writer,index=False)
            print ('\nxlsx created !')

 
# ************************** DfToPg *********************************


class DfToPG(PGToDf):
    """ [upload dataframe to postgres]

    parameter
    ---------
    dataframe: pandas-df
        A pandas df whith exact table and columns in postgres 
    """    
    def __init__(self, dataframe, table, credentials=''):
        super().__init__(table, credentials)

        self._dataframe = dataframe


    @property
    def dataframe(self):
        return self._dataframe

    @dataframe.setter
    def dataframe(self, dataframe):
        self._dataframe = dataframe

    @property
    def send_df_append(self):
        if self._credentials or self._table != '':
            a = eval(os.getenv("basesdedatos"))
            credentials=a[self._credentials]
            engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(credentials['DB_USER'], credentials['DB_PASS'], credentials['DB_IP'], credentials['DB_PORT'], credentials['DB_NAME']))
            self._dataframe.to_sql(self._table, engine, schema='public', if_exists='append', index=False)
            print ('¡Done!')

    @property
    def send_df_replace(self):
        if self._credentials or self._table != '':
            a = eval(os.getenv("basesdedatos"))
            credentials=a[self._credentials]
            engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(credentials['DB_USER'], credentials['DB_PASS'], credentials['DB_IP'], credentials['DB_PORT'], credentials['DB_NAME']))
            self._dataframe.to_sql(self._table, engine, schema='public', if_exists='replace', index=False)
            print ('¡Done!')

    @property
    def decode(self) -> pd.DataFrame():
        decodeURLs = {'%3D': '=', '%23': '#', '%20': ' ', '%27': '\'', '%22': '\"'}
        for badchar, decoded in decodeURLs.items():
            self._dataframe['URL'] = self._dataframe['URL'].apply(lambda x: x.replace(badchar, decoded))
        print ('decode done!')
        return self._dataframe
        
    @property
    def encode(self) -> pd.DataFrame():
        encodeURLs = {'=': '%3D', '#': '%23', ' ': '%20', '\'' : '%27', '\"' : '%22'}
        for badchar, encoded in encodeURLs.items():
            self._dataframe['URL'] = self._dataframe['URL'].apply(lambda x: x.replace(badchar, encoded))
        print ('encode done!')
        return self._dataframe
        
    @property
    def replace_chars(self):
        replaceInString = {'á':'a','é':'e','í':'i','ó':'o','ú':'u',
                       'Á':'A','É':'E','Í':'I','Ó':'O','Ú':'U',
                       'À':'A','È':'E','Ì':'I','Ò':'O','Ù':'U',
                       'Ä':'A','Ë':'E','Ï':'I','Ö':'O','Ü':'U',}
        for column in self._table:
            for bad_char, normal_char in replaceInString.items():
                self._dataframe[column] = self._dataframe[column].apply(lambda x: str(x).replace(bad_char, normal_char))
            # dataframe[column] = dataframe[column].apply(lambda x: str(x).strip())
        return self._dataframe
        
        

