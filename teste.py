import requests
import pandas as pd
from sqlalchemy import create_engine


url = 'https://dadosabertos.camara.leg.br/api/v2/deputados'
req = requests.get(url)
info = req.json()

df = pd.DataFrame(info['dados'], columns=["ID", "uri", "nome", "siglaPartido", "uriPartido", "siglaUf", "urlFoto", "email"])
db_connection = 'mysql+pymysql://root:007458@localhost:3306/deputados'
db_connection = create_engine(db_connection)
df.to_sql(con=db_connection, name='detalhes', if_exists='append', index=False)
#df.info()
#setData = df['ID', 'nome', 'siglaUf', 'siglaPartido', 'email', 'urlFoto']
print("dados gravados!")
