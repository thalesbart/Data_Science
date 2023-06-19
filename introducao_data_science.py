import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os, sys


#### Primeira parte ####
notas_filmes = pd.read_csv("C:/Users/Tha/DataScience-Curso/ml-latest-small/ml-latest-small/ratings.csv")
print('notas_filmes', notas_filmes)
print('colunas_notas', notas_filmes.columns)
'''
notas_filmes['rating'].head().plot(kind = 'hist')
plt.show()

filmes = pd.read_csv("C:/Users/Tha/DataScience-Curso/ml-latest-small/ml-latest-small/movies.csv")
print('filmes', filmes.head(10))

media_notas_filme_especifico = notas_filmes.loc[notas_filmes['movieId'] == 1]['rating'].mean()

print('media_notas_filme_especifico', media_notas_filme_especifico)

medias_por_filme = notas_filmes.groupby("movieId").mean()["rating"]
print(medias_por_filme.describe())
medias_por_filme.plot(kind = 'hist', title="Histograma das médias dos filmes")
plt.show()
'''
#### Segunda parte ####

filmes_tmdb = pd.read_csv("C:/Users/Tha/DataScience-Curso/archive/tmdb_5000_movies.csv")
print('filmes_tmdb', filmes_tmdb.head())
print('nomes das variaveis', filmes_tmdb.columns)

linguas_tmdb = filmes_tmdb["original_language"].unique()
# Nesse caso a lingua original é uma variavel categorica nominal
print('Linguas Originais ', linguas_tmdb)

numero_vezes_por_lingua_original = filmes_tmdb["original_language"].value_counts()

numero_vezes_por_lingua_original = numero_vezes_por_lingua_original.to_frame().reset_index()

numero_vezes_por_lingua_original.columns = ["Língua original","Quantidade de filmes"]

print('Quantidade de filmes por língua original', numero_vezes_por_lingua_original)

#sns.barplot(x = "Língua original", y = "Quantidade de filmes", data = numero_vezes_por_lingua_original )
# OU Simplesmente
#sns.catplot(x = "original_language", kind = "count", data = filmes_tmdb)

### Gráfico de pizza
'''
plt.pie(numero_vezes_por_lingua_original["Quantidade de filmes"], labels = numero_vezes_por_lingua_original["Língua original"])
plt.show()
'''
total_por_lingua = filmes_tmdb["original_language"].value_counts()
total_geral = total_por_lingua.sum()
print('total geral', total_geral)
total_filmes_ingles = total_por_lingua.loc["en"]
print('total_filmes_ingles', total_filmes_ingles)
total_resto = total_geral - total_filmes_ingles
print('total_resto', total_resto)

dados = {'lingua':['ingles','outros'], 'total': [total_filmes_ingles,total_resto]}
dados = pd.DataFrame(dados)

'''
plt.pie(dados["total"], labels = dados["lingua"])
plt.show()

sns.barplot(x='lingua', y='total', data= dados)
plt.show()
'''

exceto_ingles = filmes_tmdb.query("original_language != 'en'")
qtde_exceto_ingles = exceto_ingles["original_language"].value_counts()
print('qtde_exceto_ingles', qtde_exceto_ingles)

#sns.catplot(x="original_language", kind= "count", data = exceto_ingles, aspect = 2, order = qtde_exceto_ingles.index, palette = "GnBu_d")
#plt.show()

notas_toy_story = notas_filmes.query("movieId == 1")
notas_jumanji = notas_filmes.query("movieId == 2")

media_notas_toy_story = notas_toy_story["rating"].mean()
media_notas_jumanji = notas_jumanji["rating"].mean()

mediana_notas_toy_story = notas_toy_story["rating"].median()
mediana_notas_jumanji = notas_jumanji["rating"].median()

print('media_notas_toy_story %.2f' % media_notas_toy_story)
print('media_notas_jumanji %.2f' % media_notas_jumanji)
print('mediana_notas_toy_story %.2f' % mediana_notas_toy_story)
print('mediana_notas_jumanji %.2f' % mediana_notas_jumanji)

notas_toy_story = notas_toy_story["rating"]
notas_jumanji = notas_jumanji["rating"]

#plt.hist(notas_toy_story)
#plt.hist(notas_jumanji)

print('desvio padrão Toy Story ', notas_toy_story.std())
print('desvio padrão Jumanji ', notas_jumanji.std())

plt.boxplot([notas_toy_story, notas_jumanji])
plt.show()

