import pandas as pd
import numpy as np
import matplotlib as plot

dados_aluguel = pd.read_csv('C:/Users/Tha/DataScience-Curso/aluguel.csv', delimiter=';')
print(dados_aluguel.head())

por_tipo = dados_aluguel['Tipo'].value_counts()
print(por_tipo)

por_num_quartos = dados_aluguel.query("Tipo not in ('Conjunto Comercial/Sala', 'Loja/Salão', 'Galpão/Depósito/Armazém' )")
por_num_quartos = por_num_quartos['Quartos'].value_counts()
print(por_num_quartos)

