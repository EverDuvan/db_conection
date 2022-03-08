from PostgresDf import *

 
brandsdf= postgresToDf( 'brands','data_procesing2')
brandsdf = brandsdf.get_df
print (f'resultado: {brandsdf}')

#subida = DfToPostgres(homologadosdf,'temp_homologados','data_procesing2')
#subida.send_df_replace
# print ('objeto'.center(40,'#'))

objeto2=DfToPostgres(brandsdf,'app_product')
#objeto2.decode
objeto2.df_2_xl(brandsdf)
"""
exceldf=postgresToDf('temp_homologados.xlsx')
df=exceldf.get_xlsx_df
print(f'excel 2 df : {df}')


homologadosdf= postgresToDf('Missing_RIPLEY.xlsx')
homologadosdf = homologadosdf.get_xlsx_df

object2 =DfToPostgres(homologadosdf,'products')
df = object2.encode

object2.df_2_xl(df)

"""
