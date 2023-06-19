import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os, sys

'''
#### Primeira parte ####
notas_filmes = pd.read_csv("C:/Users/Tha/DataScience-Curso/ml-latest-small/ml-latest-small/ratings.csv")
print('colunas_notas', notas_filmes.columns)
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
sns.catplot(x = "original_language", kind = "count", data = filmes_tmdb)
plt.show()