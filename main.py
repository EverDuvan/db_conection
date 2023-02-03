from PostgresDf import *

#dataframe = PGToDf('retail_information','psql_read').get_df
#print (f'resultado: {dataframe}')

#PGToDf().df_2_xl(dataframe,'table')

#print(f'excel 2 df : {df}')
#subida = DfToPG(ready_2_process,'retail_information','data_procesing2')
#subida.send_df_replace

raw28=PGToDf('raw_scrap28.xlsx').get_xlsx_df
print (f'cantidad datos en raw_scrap28: {len(raw28)}')
#print (raw28)

raw20=PGToDf('raw_scrap20.xlsx').get_xlsx_df
print (f'cantidad datos en raw_scrap20: {len(raw20)}')
#print (raw20)


# URL en df 'raw28' que NO están en URL del df raw20

filtro1 = raw28 [~raw28['URL'].isin(raw20['URL'])]
PGToDf.df_2_xl(filtro1 , '28vs20')
print(f'devisin: {filtro1}')

#homologadosdf= PGToDf( 'homologados','data_procesing2')
#homologadosdf=homologadosdf.get_df
#print (f'cantidad datos homologados: {len(homologadosdf)}')
#print (f'resultado: {homologadosdf}')
#print(type(homologadosdf))


'''
devdf= PGToDf( 'app_product','django_db')
devdf=devdf.get_df
print (f'cantidad datos en dev: {len(devdf)}')
#print (f'df api: {devdf}')
#print (type(devdf))

filtro1 = homologadosdf [~homologadosdf['HASH_ID'].isin(ready_2_process['HASH_ID'])]
PGToDf.df_2_xl('homovsready', filtro1) 
#print(f'devisin: {filtro1}')
#print (f'devdfisin len: {len(devdf)}')

filtro2 = homologadosdf[~homologadosdf['HASH_ID'].isin(devdf['HASH_ID'])]
PGToDf.df_2_xl('homovsdev', filtro2) 
#print (dfisin)
#print (f'dfisin: {filtro2}')

#subida = DfToPostgres(homologadosdf,'temp_homologados','data_procesing2')
#subida.send_df_replace
# print ('objeto'.center(40,'#'))

#homologadosdf= PGToDf( 'app_retailer','django_db').get_df
#print (f'resultado: {homologadosdf}')
#objeto2=DfToPG(homologadosdf,'drogor').df_2_xl(homologadosdf)
#objeto2.encode


 '''