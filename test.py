def send_df_append(self):
    if self._credentials or self._table != '':
        a = eval(os.getenv("basesdedatos"))
        credentials=a[self._credentials]
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(credentials['DB_USER'], credentials['DB_PASS'], credentials['DB_IP'], credentials['DB_PORT'], credentials['DB_NAME']))
        self._dataframe.to_sql(self._table, engine, schema='public', if_exists='append', index=False)
        print ('¡Done!')

    @property
    def get_df(self):
        a = eval(os.getenv("basesdedatos"))
        credentials=a[self._credentials]
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(credentials['DB_USER'], credentials['DB_PASS'], credentials['DB_IP'], credentials['DB_PORT'], credentials['DB_NAME']))
        query= f'SELECT * from {self._table}'
        df = pd.read_sql_query(query, engine)
        #df = self.load_df(credentials['DB_USER'], credentials['DB_PASS'], credentials['DB_IP'], credentials['DB_PORT'], credentials['DB_NAME'], query)
        if self._column != []:
            df=df[self._column]
        return df

    def load_df(self,DB_USER: str, DB_PASS: str, DB_IP: str,
                    DB_PORT: int, DB_NAME: str, query: str) -> pd.DataFrame():
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_IP}:{DB_PORT}/{DB_NAME}')
        #query= f'SELECT * from {self._table}'
        dataframe = pd.read_sql_query(query, engine)
        return dataframe

        