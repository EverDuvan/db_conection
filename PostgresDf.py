import pandas as pd
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv
import os
import xlsxwriter
load_dotenv()
                     

class postgresToDf:
    """ [Download data from postgres to a dataframe]

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

    def __init__(self, credentials, table, column=[]):

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
    def dataframe(self):
        return self._dataframe

    @dataframe.setter
    def dataframe(self, dataframe):
        self._dataframe = dataframe  
    
    def load_df(self,DB_USER: str, DB_PASS: str, DB_IP: str,
                    DB_PORT: int, DB_NAME: str, query: str) -> pd.DataFrame():
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(DB_USER, DB_PASS, DB_IP, DB_PORT, DB_NAME))
        #conexion = create_engine(engine)
        query='SELECT * from {}'.format(self._table)
        dataframe = pd.read_sql_query(query, engine)
        return dataframe
    
    @property
    def get_df(self):
        a = eval(os.getenv("basesdedatos"))
        credentials=a[self._credentials]
        query='SELECT * from {}'.format(self._table)
        df = self.load_df(credentials['DB_USER'], credentials['DB_PASS'], credentials['DB_IP'], credentials['DB_PORT'], credentials['DB_NAME'], query)
        if self._column != []:
            df=df[self._column]
        return df

    

    @property
    def table_name(self):
        print (f'table: {self._table}')

 
# ************************** DfToPostgres *********************************


class DfToPostgres(postgresToDf):
    """ [upload dataframe to postgres]

    parameter
    ---------
    dataframe: pandas-df
        A pandas df whith exact table and columns in postgres 
    """    
    def __init__(self, dataframe, table='', credentials=''):
        super().__init__(credentials, table)

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
    def df_2_xl(self) -> None:
        if self._table == '':
            self._table = 'file'
        full_path = "{}.xlsx".format(self._table)
        with pd.ExcelWriter(full_path,
                            engine='xlsxwriter',
                            options={'strings_to_urls': False}) as writer:
            self._dataframe.to_excel(writer)
            print ('\nxlsx created !')

    @property
    def decode(self) -> pd.DataFrame():
        decodeURLs = {'%3D': '=', '%23': '#', '%20': ' ', '%27': '\'', '%27': '\"'}
        for badchar, decoded in decodeURLs.items():
            self._dataframe['URL'] = self._dataframe['URL'].apply(lambda x: x.replace(badchar, decoded))
        print ('decode done!')
        return self._dataframe
        
    @property
    def encode(self) -> pd.DataFrame():
        encodeURLs = {'=': '%3D', '#': '%23', ' ': '%20', '\'' : '%27', '\"' : '%27'}
        for badchar, encoded in encodeURLs.items():
            self._dataframe['URL'] = self._dataframe['URL'].apply(lambda x: x.replace(badchar, encoded))
        print ('encode done!')
        return self._dataframe
        

