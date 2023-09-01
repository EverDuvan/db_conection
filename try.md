def text_2_hash(dataframe: pd.DataFrame, column: str) -> str:
    """
    Text goes to bytes and get encoded un utf-8 for stadarization.
    """
    if column == 'URL':
        current_str = dataframe[column].lower()
    else:
        current_str = dataframe[column]

