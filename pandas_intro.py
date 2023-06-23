import pandas as pd
import numpy as np
import matplotlib as plot

'''
dados_json = pd.read_json('C:/Users/Tha/DataScience-Curso/extras/aluguel.json')
#print(dados_json)

dados_html_1 = pd.read_html('C:/Users/Tha/DataScience-Curso/extras/dados_html_2.html')
# é possível também passar um url
#print(dados_html_1)

dados_aluguel = pd.read_csv('C:/Users/Tha/DataScience-Curso/aluguel.csv', delimiter=';')
#print(dados_aluguel.head())

#print(dados_aluguel.dtypes)

tipos_imoveis = dados_aluguel['Tipo'].drop_duplicates()
### Redefinindo o Index
tipos_imoveis.index = range(tipos_imoveis.shape[0])
tipos_imoveis =  pd.DataFrame(tipos_imoveis,columns=['Tipo'])
print(tipos_imoveis)

imoveis_residenciais = list(tipos_imoveis['Tipo'])

imoveis_residenciais = ['Quitinete', 'Casa', 'Apartamento', 'Casa de Condomínio', 'Flat', 'Casa de Vila', 'Loft', 'Studio']
print(imoveis_residenciais)

selecao_imoveis_residenciais = dados_aluguel['Tipo'].isin(imoveis_residenciais)
print(selecao_imoveis_residenciais)

dados_imoveis_residenciais = dados_aluguel[selecao_imoveis_residenciais]
print(dados_imoveis_residenciais)

dados_imoveis_residenciais.index = range(dados_imoveis_residenciais.shape[0])

dados_imoveis_residenciais.to_csv('C:/Users/Tha/DataScience-Curso/extras/dados_imoveis_residenciais.csv', sep = ';', index=False)

por_tipo = dados_aluguel['Tipo'].value_counts()
#print(por_tipo)

#imoveis_residenciais = tipos_imoveis.query("Tipo not in ")

por_num_quartos = dados_aluguel.query("Tipo not in ('Conjunto Comercial/Sala', 'Loja/Salão', 'Galpão/Depósito/Armazém' )")
por_num_quartos = por_num_quartos['Quartos'].value_counts()
#print(por_num_quartos)

dados_imoveis_residenciais = pd.read_csv('C:/Users/Tha/DataScience-Curso/extras/dados_imoveis_residenciais.csv', sep=';')
print(dados_imoveis_residenciais)

ops, agora vai
'''









