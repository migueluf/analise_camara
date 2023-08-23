import requests
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine



url = 'https://dadosabertos.camara.leg.br/api/v2/deputados'
req = requests.get(url)
info = req.json()

df = pd.DataFrame(info['dados'], columns=["id"])
#insira aqui as cedenciais do banco
db_connection = 'mysql+pymysql://root:senha@localhost:3306/deputados'
db_connection = create_engine(db_connection)
for u in df['id']:
    urlDetalhe = f'https://dadosabertos.camara.leg.br/api/v2/deputados/{u}'
    req2 = requests.get(urlDetalhe)
    inform = req2.json()
    df = json_normalize(inform['dados']['ultimoStatus'])
    df1 = json_normalize(inform['dados']['ultimoStatus']['gabinete'])
    df2 = json_normalize(inform['dados'])
    tel = ['telefone']
    dt = ['dataNascimento']
    df1inf = df1.filter(items=tel)
    df2inf = df2.filter(items=dt)
    dfgeral = pd.concat([df,df1inf,df2inf], axis=1)
    colunas = ['id','uri','nome', 'telefone','siglaUf','siglaPartido','email','urlFoto','dataNascimento','ufNascimento']
    mostrar = dfgeral.filter(items=colunas)
    print(mostrar['id'])
    print(mostrar['nome'])
    #envio de dados para o banco no campo name insira o nome que será a tabela
    mostrar.to_sql(con=db_connection, name='detalhes', if_exists='append', index=False)
    
    #mostrar.info()
#setData = df['id']
#print(df)
#df.info()
