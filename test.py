import pandas as pd 
import openpyxl

x = 'raw_scrap20.xlsx'
y = 'raw_scrap28.xlsx'

def xltodf(xlsxfile):
    df = pd.read_excel(xlsxfile , engine='openpyxl', na_filter = False)
    print (f'cantidad datos en {xlsxfile} : {len(df)}')
    #print (df)
    return df

def df_2_xl(df,table):
    full_path = f'{table}.xlsx'
    with pd.ExcelWriter(full_path,
                        engine='xlsxwriter',
                        engine_kwargs={'options':{'strings_to_urls': False}}) as writer:
        df.to_excel(writer,index=False)

x = xltodf(x)
y = xltodf(y)

filtro = y [~y['URL'].isin(x['URL'])] # URL en df 'y' que NO están en URL del df 'x'
df_2_xl(filtro , 'filtro')
print (f'cantidad datos en filtro : {len(filtro)}')
#print(f'filtro : {filtro}')

