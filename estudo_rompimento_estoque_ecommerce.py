import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from data_profiling import ProfileReport
import plotly.express as px

path_bases = "C:/Users/Thales/OneDrive - Manager Engenharia Ltda/Área de Trabalho/MBA - Data Science & Analytics/Projects/Dados/"

df_clientes = pd.read_csv(path_bases+'olist_customers_dataset.csv')
df_geolocalizacao = pd.read_csv(path_bases+'olist_geolocation_dataset.csv')
df_itens_vendas = pd.read_csv(path_bases+'olist_order_items_dataset.csv')
df_pagamentos = pd.read_csv(path_bases+'olist_order_payments_dataset.csv')
df_reviews_compras = pd.read_csv(path_bases+'olist_order_reviews_dataset.csv')
df_vendas = pd.read_csv(path_bases+'olist_orders_dataset.csv')
df_produtos = pd.read_csv(path_bases+'olist_products_dataset.csv')
df_vendedores = pd.read_csv(path_bases+'olist_sellers_dataset.csv')
df_categorias_produtos = pd.read_csv(path_bases+'product_category_name_translation.csv')

# Orders + Items
df_geral = df_itens_vendas.merge(
    df_vendas,
    on="order_id",
    how="left"
)

# Produtos
df_geral = df_geral.merge(
    df_produtos,
    on="product_id",
    how="left"
)

# Clientes
df_geral = df_geral.merge(
    df_clientes,
    on="customer_id",
    how="left"
)

df_geral["data_venda"] = pd.to_datetime(
    df_geral["order_purchase_timestamp"]
)

df_geral = df_geral.loc[(df_geral['data_venda'] >= '2017-01-01')&(df_geral['data_venda'] <= '2017-12-31')]

'''
profile = ProfileReport(
    df_geral,
    title="EDA Vendas",
    explorative=True
)

profile.to_file("C:/Users/Thales/OneDrive - Manager Engenharia Ltda/Área de Trabalho/MBA - Data Science & Analytics/Projects/eda_vendas.html")
'''
vendas_categoria = (
    df_geral.groupby(
        ["data_venda", "product_category_name"]
    )["price"]
    .sum()
    .reset_index()
)

# =========================
# TRANSFORMAR EM PIVOT
# =========================

pivot = vendas_categoria.pivot(
    index="data_venda",
    columns="product_category_name",
    values="price"
).fillna(0)

top_categorias = (
    df_geral.groupby("product_category_name")["price"]
    .sum()
    .sort_values(ascending=False)
    .head(5)
    .index
)

pivot = pivot[top_categorias]

# =========================
# PLOTAR GRÁFICO
# =========================

plt.figure(figsize=(18,8))

for coluna in pivot.columns:
    plt.plot(
        pivot.index,
        pivot[coluna],
        label=coluna
    )

plt.title("Vendas por Dia por Categoria")
plt.xlabel("Data")
plt.ylabel("Valor Vendido")

plt.xticks(rotation=45)

plt.grid(True)

plt.legend(
    bbox_to_anchor=(1.02, 1),
    loc="upper left"
)

plt.tight_layout()

#plt.show()



grafico = (
    df_geral.groupby("product_photos_qty")
    .agg(
        quantidade_vendida=("order_id", "count")
    )
    .reset_index()
)

# =========================
# REMOVER NULOS
# =========================

grafico = grafico.dropna()

# =========================
# ORDENAR
# =========================

grafico = grafico.sort_values(
    by="product_photos_qty"
)

# =========================
# GRÁFICO
# =========================

fig = px.bar(
    grafico,
    x="product_photos_qty",
    y="quantidade_vendida",
    title="Quantidade Vendida por Quantidade de Fotos",
    labels={
        "product_photos_qty": "Quantidade de Fotos",
        "quantidade_vendida": "Quantidade Vendida"
    }
)

fig.show()