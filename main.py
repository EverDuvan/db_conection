from PostgresDf import *

homologadosdf= postgresToDf('data_procesing2', 'homologados')
homologadosdf = homologadosdf.get_df
#print (f'homologados: {homologadosdf}')

objeto2=DfToPostgres(homologadosdf,'temp_homologados')
objeto2.decode
objeto2.df_2_xl

