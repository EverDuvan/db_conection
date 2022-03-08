def text_2_hash(dataframe: pd.DataFrame, column: str) -> str:
    """
    Text goes to bytes and get encoded un utf-8 for stadarization.
    """
    if column == 'URL':
        current_str = dataframe[column].lower()
    else:
        current_str = dataframe[column]

    hash_obj = hashlib.sha1(bytes(current_str, encoding='utf-8'))
    hex_dig = hash_obj.hexdigest()

    return str(hex_dig)

def calculate_hash_scrap(dataframe: pd.DataFrame):
    dataframe['HASH_ID'] = dataframe.apply(lambda row: text_2_hash(row, 'URL'), axis=1)

    return dataframe

    basesdedatos = "{'data_procesing2' : {'DB_USER':'everduvan', \
            'DB_PASS':'030481',\
            'DB_IP':'localhost',\
            'DB_PORT':'5432',\
            'DB_NAME':'data_procesing2',\
        },  'data_processing' : {'DB_USER':'everduvan', \
            'DB_PASS':'030481',\
            'DB_IP':'localhost',\
            'DB_PORT':'5432',\
            'DB_NAME':'data_processing'}}"


    basesdedatos = "{'data_procesing2' : {'DB_USER':'everduvan', \
            'DB_PASS':'030481',\
            'DB_IP':'localhost',\
            'DB_PORT':'5432',\
            'DB_NAME':'data_procesing2',\
        },  'data_processing' : {'DB_USER':'everduvan', \
            'DB_PASS':'030481',\
            'DB_IP':'localhost',\
            'DB_PORT':'5432',\
            'DB_NAME':'data_processing',\
        },  'data_processing' : {'DB_USER':'LeAMerSiON', \
            'DB_PASS':'wa#5HEYHs65GEvp',\
            'DB_IP':'mobile2.hitchme.app',\
            'DB_PORT':'5433',\
            'DB_NAME':'LeAMerSiON'}}"