import pandas as pd 

# dataframe1
df1 = pd.DataFrame({'col1': [4, 2, 3,'A','C'], 'col2': [4, 5, 6,'B','D'] , 'col3': [1, 2, 3, 4, 5], 'valor': [1, 2, 3,'A','C']})
print (df1)
# dataframe2
df2 = pd.DataFrame({'col1': [1, 2, 3, 4,'D'], 'col2': [4, 5, 6, 7,'D'], 'col3': [1, 2, 3,'A','C']})
print (df2)
# Comprobación de coincidencia y asignación de nueva columna
#df2['coincidencia'] = df2[['columna1', 'columna2']].isin(df1[['columna1', 'columna2']]).all(axis=1).astype(int)

merged_df = pd.merge(df2, df1[['col1', 'col2', 'valor']], on=['col1', 'col2'], how='left')
print(merged_df)

merged_df = merged_df.drop(['col3'], axis=1)
print (merged_df)
