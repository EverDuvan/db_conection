from PostgresDf import *

homologadosdf = PGToDf( 'product_homologated','data_procesing2', 'MARCA').get_df
print (f'resultado: {homologadosdf}')
objeto2=PGToDf('table').df_2_xl(homologadosdf,'table')



#homologadosdf= PGToDf( 'homologados','data_procesing2')
#homologadosdf=homologadosdf.get_df
#print (f'cantidad datos homologados: {len(homologadosdf)}')
#print (f'resultado: {homologadosdf}')
#print(type(homologadosdf))
''' 
ready_2_process=PGToDf('ready_2_process.xlsx')
ready_2_process = ready_2_process.get_xlsx_df
print (f'cantidad datos ready2process: {len(ready_2_process)}')
#print(f'excel 2 df : {df}')

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