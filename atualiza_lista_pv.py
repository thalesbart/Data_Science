import tkinter
from numpy import append
import pandas as pd
import sys, os
from pandas.core.accessor import register_dataframe_accessor
from pandas.core.frame import DataFrame
import requests
from requests import auth
from requests.api import get
from requests.models import ContentDecodingError
import csv, json
import math
import time
import pyodbc
from requests.auth import HTTPBasicAuth
import base64
from sys import exit
from time import sleep
from pandas import json_normalize
import threading
import time
from tkinter import Text, filedialog
from sqlalchemy.engine import URL
from sqlalchemy.engine import create_engine
from datetime import date
from datetime import datetime, timedelta
from lxml import etree
import io

headers_pipefy = {
                    "Accept": "application/json",
                    "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyIjp7ImlkIjo5NjExMjYsImVtYWlsIjoibWFya2V0aW5nQHphbm90dGlyZWZyaWdlcmFjYW8uY29tLmJyIiwiYXBwbGljYXRpb24iOjMwMDEzNDkxM319.VIgfDCDN7Vb_HpBbBCqNi0cyoJT5AeDU_tT5OhZv6IBbkqRYTMYyfhFobu4d7KrSAYGRN7BKDlT2Ls_rO5ZK2A",
                    "Content-Type": "application/json"
                }

url_pipefy = "https://api.pipefy.com/graphql"

#### ALTERAR PARA BASE DE PRODUÇÃO
server = '192.168.1.15'
database = 'PROTHEUS_ZANOTTI_HOMOLOGA'
username = 'totvs' 
password = 'totvsip'
conn1 = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
cursor = URL.create("mssql+pyodbc", query={"odbc_connect": conn1})
conn = create_engine(cursor)

def envia_arquivo_pipe_compras(nome_arquivo, caminho_arquivo, id_card_compras):
    arquivo = io.open(str(caminho_arquivo)+str(nome_arquivo),'rb', buffering = 0)
    arquivo = arquivo.read()
    payload_arquivo = {"query":"mutation {createPresignedUrl(input: {fileName: \""+str(nome_arquivo)+"\", organizationId: 300452242}) {clientMutationId downloadUrl url}}"}
    request_arquivo = requests.request("POST", url_pipefy, json = payload_arquivo, headers = headers_pipefy)
    request_arquivo = json.loads(request_arquivo.content)
                                            
    url_arquivo1 = request_arquivo['data']['createPresignedUrl']['url']
    local_final_arquivo1 = str(url_arquivo1).split("?")[0]
    local_final_arquivo = str(url_arquivo1).find('?')
    url_arquivo = url_arquivo1[46:local_final_arquivo]

    request_put = requests.put(url_arquivo1, data = arquivo)
                  
    payload_atualiza_card_lista_pv_compras = {"query": "mutation {updateCardField(input:{card_id:\""+str(id_card_compras)+"\", field_id: \"solicita_o_de_compras\", new_value: [\""+str(url_arquivo)+"\"]}) {clientMutationId  success}}"}                   
    request_anexa_lista_pv_compras = requests.post(url_pipefy, json = payload_atualiza_card_lista_pv_compras, headers = headers_pipefy)

def cria_atualiza_card_pipefy_compras(atendimento,pedido, cliente, filial):
        payload_criar_card_compras = {"query":"mutation {createCard(input:\
                                    {pipe_id: 303509100, title:\""+str(pedido)+"\"\
                                    fields_attributes:[\
                                    {field_id:\"n_mero_do_atendimento\", field_value:\""+str(atendimento)+"\"},\
                                    {field_id:\"nome_do_cliente\", field_value:\""+str(cliente)+"\"},\
                                    {field_id: \"filial_do_atendimento\", field_value:\""+str(filial)+"\"}]}){card{id title}}}"}        
    
        response_criar_card_compras = requests.request("POST", url_pipefy, json = payload_criar_card_compras, headers = headers_pipefy)
        criar_card_compras = json.loads(response_criar_card_compras.content)
        criar_card_compras = pd.DataFrame.from_dict(json_normalize(criar_card_compras), orient='columns')
        return(criar_card_compras)

def buscar_card(url_pipefy, headers_pipefy, id_pipe, titulo_card, atendimento):
    payload_card_pipefy = {"query": "{cards(pipe_id:"+str(id_pipe)+", search:{title:\""+str(titulo_card).strip()+"\"}) {  edges {  node{ id title current_phase {id name} fields {name report_value updated_at value field{id}}}}}}"}
    request_card_pipefy = requests.request("POST", url_pipefy, json = payload_card_pipefy, headers = headers_pipefy)
    response_card_pipefy = json.loads(request_card_pipefy.content)

    response_card_pipefy = pd.DataFrame.from_dict(json_normalize(response_card_pipefy), orient='columns')

    if response_card_pipefy.empty == False:
            response_card_pipefy = response_card_pipefy['data.cards.edges'][0]
            
            response_card_pipefy = pd.DataFrame.from_dict(json_normalize(response_card_pipefy), orient='columns')
        
            if response_card_pipefy.empty == False:                
                qtde_response_card_pipefy = response_card_pipefy['node.id'].count()
                linha_response_card_pipefy = 0

                while linha_response_card_pipefy < qtde_response_card_pipefy:
                        cards_entregas_verifica_existe_card = response_card_pipefy.iloc[linha_response_card_pipefy,4]
                        
                        cards_entregas_verifica_existe_card1 = pd.DataFrame.from_dict(json_normalize(cards_entregas_verifica_existe_card),orient='columns')
                        atd_card_verifica_existe_card = cards_entregas_verifica_existe_card1.loc[cards_entregas_verifica_existe_card1['name'] == 'Número do atendimento']
                        
                        if atd_card_verifica_existe_card.empty == False:
                            atd_card_verifica_existe_card2 = str(atd_card_verifica_existe_card.iloc[0,1])

                            if str(atd_card_verifica_existe_card2) == str(atendimento).zfill(6):
                                id_card_pipefy = response_card_pipefy['node.id'].iloc[0]
                                infos_card = pd.DataFrame.from_dict(json_normalize(cards_entregas_verifica_existe_card),orient='columns')
                                
                            else:                   
                                id_card_pipefy = 'Não encontrado'
                                infos_card = 'Não encontrado'

                        if atd_card_verifica_existe_card.empty:
                            id_card_pipefy = 'Não encontrado'
                            infos_card = 'Não encontrado'
                        
                        linha_response_card_pipefy = linha_response_card_pipefy + 1    
            else:  
                id_card_pipefy = 'Não encontrado'
                infos_card = 'Não encontrado'
                
    else:
            id_card_pipefy = 'Não encontrado'
            infos_card = 'Não encontrado'
    return(id_card_pipefy, infos_card)

def comparar_lista_com_pv(pv, atd_itens, tabela_original):
    pv_bd = "SELECT SC6.C6_PRODUTO, SB1.B1_ZDESRDZ, SC6.C6_QTDVEN, SC6.C6_QTDLIB, (SC6.C6_QTDVEN - SC6.C6_QTDLIB) AS QTDE_RESTANTE, SC6.D_E_L_E_T_ AND FROM SC6010 SC6 WITH (NOLOCK)\
             LEFT JOIN SB1010 SB1 ON SB1.D_E_L_E_T_ = '' AND SB1.B1_COD = SC6.C6_PRODUTO\
             WHERE SC6.C6_NUM = '"+str(pv)+"' AND SC6.C6_FILIAL = '0101'"
    pv_bd = pd.read_sql(pv_bd, conn)

    lista_atual_pv = pd.DataFrame(columns=['','','','','','','','','','',''])

    cab_pv = "SELECT SA1.A1_NOME, SA3.A3_NOME, SUA.UA_ZATMFIN FROM SC5010 SC5 WITH (NOLOCK)\
                LEFT JOIN SA1010 SA1 ON SA1.D_E_L_E_T_ = '' AND SC5.C5_CLIENTE = SA1.A1_COD AND SUBSTRING(SC5.C5_FILIAL,1,2) = SA1.A1_FILIAL\
                LEFT JOIN SA3010 SA3 ON SA3.D_E_L_E_T_ = '' AND SA3.A3_COD = SC5.C5_VEND1\
                LEFT JOIN SUA010 SUA ON SUA.D_E_L_E_T_ = '' AND SUA.UA_ZPEDITENS = SC5.C5_NUM\
                WHERE SC5.D_E_L_E_T_ = '' AND SC5.C5_NUM = '"+str(pv)+"' AND SC5.C5_FILIAL = '0101'"
    cab_pv = pd.read_sql(cab_pv, conn)

    nome_cliente = str(cab_pv.iloc[0,0])
    nome_vendedor = str(cab_pv.iloc[0,1])
    atd_fin = str(cab_pv.iloc[0,2])
    
    lista_atual_pv.loc[0] = ['PV', 'Atd. Itens', 'Cliente', 'Vendedor','','','','','','','']
   
    lista_atual_pv.loc[1] = [pv, atd_itens, nome_cliente, nome_vendedor,'','','','','','','']
    lista_atual_pv.loc[2] = ['', '', '', '','','','','','','','']
    lista_atual_pv.loc[3] = ['Envios','Prioridade','Cod. Prod','Produto','Qtde', 'Qtde Lib', 'Qtde rest','Qtde Env','Status', 'RP', 'Atualização']
    
    #### VERIFICA PRODUTOS DE LISTA PROVENIENTE PIPEFY COM PV_BD 
    '''
    linha_tab_original = 0
    for prod in tabela_original['Cod. Prod.']:
        prod_bd = pv_bd.loc[pv_bd['C6_PRODUTO'] == str(prod)]

        if prod_bd.empty:
            qtd_tab_orig = prod_bd.iloc[0,4]
            envios = prod_bd.iloc[0,0]
            ordem = prod_bd.iloc[0,1]
            status = 'EXCLUíDO'
            rp = prod_bd.iloc[0,9]
        
        tabela_original.loc[linha_tab_original]= [envios, ordem, prod, produto, qtd_vend, qtd_lib_post, qtd_rest, qtde_env, status, rp, apont_atualizacao]

        linha_tab_original = linha_tab_original + 1
        
        lista_atual_pv = 

        '''



    ##### VERIFICA PRODUTOS PV_BD E COMPARA COM LISTA PROVENIENTE PIPEFY
    linha = 0
    linha_planilha = 4
    atualiza_compras = 0
    for cod_produto in pv_bd['C6_PRODUTO']:        
        qtd_vend = pv_bd.iloc[linha, 2]
        qtd_lib = pv_bd.iloc[linha, 3]
        qtd_rest = pv_bd.iloc[linha, 4]
        produto = pv_bd.iloc[linha,1]
        deletado = pv_bd.iloc[linha, 5]
        
        prod_tab_orig = tabela_original.loc[tabela_original['Cod. Prod'] == str(cod_produto)]

        if prod_tab_orig.empty == False and str(deletado) == '':
            qtd_tab_orig = prod_tab_orig.iloc[0,4]
            envios = prod_tab_orig.iloc[0,0]
            ordem = prod_tab_orig.iloc[0,1]
            status = prod_tab_orig.iloc[0,8]
            rp = prod_tab_orig.iloc[0,9]

            if qtd_vend != qtd_tab_orig:                          
                apont_atualizacao = 'de: '+str(qtd_tab_orig)+' para: '+str(qtd_vend)                
            else:
                apont_atualizacao = ''
            
            if qtd_rest ==  0:
                status = 'LIBERADO'
            
            if  qtd_rest !=  0 and "ISO" in produto:
                status = 'PRODUÇÃO'
            
            if qtd_rest !=  0 and "ISO" not in produto:
                atualiza_compras = 1
                status = "SOL. COMPRA"

                card_compras = buscar_card(url_pipefy, headers_pipefy, '303509100', nome_cliente, atd_fin)[0]

                if str(card_compras) == 'Não encontrado':
                    card_compras = cria_atualiza_card_pipefy_compras(atd_fin,pv, nome_cliente, '0101')
        
        if prod_tab_orig.empty == False and str(deletado) == '*':
            status = 'INCLUÍDO' 
            apont_atualizacao = ''
            qtd_tab_orig = prod_tab_orig.iloc[0,4]
            envios = prod_tab_orig.iloc[0,0]
            ordem = prod_tab_orig.iloc[0,1]
            rp = prod_tab_orig.iloc[0,9]

        if prod_tab_orig.empty == True and str(deletado) == '':
            status = 'INCLUÍDO' 
            apont_atualizacao = ''
            envios = ''
            ordem = '' 

        select_rp = "SELECT D2_DOC, D2_QUANT FROM SD2010 WITH (NOLOCK) WHERE D2_SERIE = 'RP' AND D2_COD = '"+str(cod_produto)+"' AND D2_PEDIDO = '"+str(pv)+"'"
        select_rp = pd.read_sql(select_rp, conn)

        if select_rp.empty == False:
            linha_rp = 0
            rp_geral = ""
            qtde_env = 0
            for rp_in in select_rp['D2_DOC']:
                qtde_env_in = select_rp.iloc[linha_rp,1]
                rp = "RP: " + str(rp) + "Qtde: "+str(qtde_env_in)
                rp_geral = rp_geral + ", " + rp
                qtde_env = qtde_env + qtde_env_in                

                linha_rp = linha_rp + 1
        else:
            rp = ''
            qtde_env = ''

        liberacoes = "SELECT C9_PRODUTO, C9_QTDLIB FROM SC9010 WITH (NOLOCK) WHERE D_E_L_E_T_ = '' AND C9_PEDIDO = '"+str(pv)+"' AND C9_FILIAL = '0101' AND C9_PRODUTO = '"+str(cod_produto)+"'"
        liberacoes = pd.read_sql(liberacoes, conn)

        if liberacoes.empty == False:
            qtd_lib_post = liberacoes.iloc[0,1]
            qtd_rest = qtd_vend - qtd_lib_post
        else:
            qtd_lib_post = qtd_lib
            
        lista_atual_pv.loc[linha_planilha] = [envios, ordem, cod_produto, produto, qtd_vend, qtd_lib_post, qtd_rest, qtde_env, status, rp, apont_atualizacao]

        linha = linha + 1
        linha_planilha = linha_planilha + 1

    nome_arquivo_eng = "lista_pv_"+str(pv)+".xlsx"
    caminho_arquivo = "//192.168.1.16/02 - Público/09 - Marketing/Thales/listas_pvs/"
    lista_atual_pv.to_excel(str(caminho_arquivo)+str(nome_arquivo_eng), index=False)

    if atualiza_compras == 1:
        card_compras = buscar_card(url_pipefy, headers_pipefy, '303509100', nome_cliente, atd_fin)[0]
        envia_arquivo_pipe_compras(nome_arquivo_eng, caminho_arquivo, card_compras)
    return(lista_atual_pv)

def mover_card(id_card, id_fase_destino):
    payload_pipefy_move_card = {"query":"mutation {moveCardToPhase(input:{card_id:"+str(id_card)+",destination_phase_id:"+str(id_fase_destino)+"}){card{id}}}"}       
    response_pipefy_move_card = requests.request("POST", url_pipefy, json = payload_pipefy_move_card, headers = headers_pipefy)
    df_pipefy_move_card = json.loads(response_pipefy_move_card.content)
    return(df_pipefy_move_card)

def envia_arquivo_pipe_projetos(nome_arquivo, caminho_arquivo, id_card_eng):
    arquivo = io.open(str(caminho_arquivo)+str(nome_arquivo),'rb', buffering = 0)
    arquivo = arquivo.read()
    payload_arquivo = {"query":"mutation {createPresignedUrl(input: {fileName: \""+str(nome_arquivo)+"\", organizationId: 300452242}) {clientMutationId downloadUrl url}}"}
    request_arquivo = requests.request("POST", url_pipefy, json = payload_arquivo, headers = headers_pipefy)
    request_arquivo = json.loads(request_arquivo.content)
                                            
    url_arquivo1 = request_arquivo['data']['createPresignedUrl']['url']
    local_final_arquivo1 = str(url_arquivo1).split("?")[0]
    local_final_arquivo = str(url_arquivo1).find('?')
    url_arquivo = url_arquivo1[46:local_final_arquivo]

    request_put = requests.put(url_arquivo1, data = arquivo)
                  
    payload_atualiza_card_lista_pv_eng = {"query": "mutation {updateCardField(input:{card_id:\""+str(id_card_eng)+"\", field_id: \"lista_sem_altera_es_para_o_depto_de_engenharia\", new_value: [\""+str(url_arquivo)+"\"]}) {clientMutationId  success}}"}                   
    request_anexa_lista_pv_eng = requests.post(url_pipefy, json = payload_atualiza_card_lista_pv_eng, headers = headers_pipefy)

def envia_arquivo_pipe_logistica(nome_arquivo, caminho_arquivo, id_card_log):
    ##here
    arquivo = io.open(str(caminho_arquivo)+str(nome_arquivo),'rb', buffering = 0)
    arquivo = arquivo.read()
    payload_arquivo = {"query":"mutation {createPresignedUrl(input: {fileName: \""+str(nome_arquivo)+"\", organizationId: 300452242}) {clientMutationId downloadUrl url}}"}
    request_arquivo = requests.request("POST", url_pipefy, json = payload_arquivo, headers = headers_pipefy)
    request_arquivo = json.loads(request_arquivo.content)
                                            
    url_arquivo1 = request_arquivo['data']['createPresignedUrl']['url']
    local_final_arquivo1 = str(url_arquivo1).split("?")[0]
    local_final_arquivo = str(url_arquivo1).find('?')
    url_arquivo = url_arquivo1[46:local_final_arquivo]

    request_put = requests.put(url_arquivo1, data = arquivo)
                  
    payload_atualiza_card_lista_pv_log = {"query": "mutation {updateCardField(input:{card_id:\""+str(id_card_log)+"\", field_id: \"lista_log_stica\", new_value: [\""+str(url_arquivo)+"\"]}) {clientMutationId  success}}"}                   
    request_anexa_lista_pv_log = requests.post(url_pipefy, json = payload_atualiza_card_lista_pv_log, headers = headers_pipefy)

def envia_arquivo_pipe_pcp(nome_arquivo, caminho_arquivo, id_card_pcp):
    arquivo = io.open(str(caminho_arquivo)+str(nome_arquivo),'rb', buffering = 0)
    arquivo = arquivo.read()
    payload_arquivo = {"query":"mutation {createPresignedUrl(input: {fileName: \""+str(nome_arquivo)+"\", organizationId: 300452242}) {clientMutationId downloadUrl url}}"}
    request_arquivo = requests.request("POST", url_pipefy, json = payload_arquivo, headers = headers_pipefy)
    request_arquivo = json.loads(request_arquivo.content)
                                            
    url_arquivo1 = request_arquivo['data']['createPresignedUrl']['url']
    local_final_arquivo1 = str(url_arquivo1).split("?")[0]
    local_final_arquivo = str(url_arquivo1).find('?')
    url_arquivo = url_arquivo1[46:local_final_arquivo]

    request_put = requests.put(url_arquivo1, data = arquivo)
                  
    payload_atualiza_card_lista_pv_pcp = {"query": "mutation {updateCardField(input:{card_id:\""+str(id_card_pcp)+"\", field_id: \"anexo_enviado_pela_engenharia\", new_value: [\""+str(url_arquivo)+"\"]}) {clientMutationId  success}}"}                   
    request_anexa_lista_pv_pcp = requests.post(url_pipefy, json = payload_atualiza_card_lista_pv_pcp, headers = headers_pipefy)

def fluxo_lista_eng():
    #### Lista Atualizada Engenharia
    pedidos_lista_atualizadas = "SELECT SA1.A1_NOME , C5_ZATU_LI, SUA.UA_ZATMFIN, SUA.UA_ZPEDITENS, SUA.UA_NUM FROM SC5010 SC5\
                                    LEFT JOIN SUA010 SUA ON SUA.D_E_L_E_T_ = '' AND SUA.UA_ZPEDITENS = SC5.C5_NUM\
                                    LEFT JOIN SA1010 SA1 ON SA1.D_E_L_E_T_ = '' AND SA1.A1_COD = SUA.UA_CLIENTE\
                                    WHERE C5_ZATU_LI = 'ATU_ENG'"
    pedidos_lista_atualizadas = pd.read_sql(pedidos_lista_atualizadas, conn)

    linha = 0
    for atm_fin in pedidos_lista_atualizadas['UA_ZATMFIN']:
        atd_itens = pedidos_lista_atualizadas.iloc[linha, 4]
        cliente = pedidos_lista_atualizadas.iloc[linha,0]

        infos_card_eng = buscar_card(url_pipefy, headers_pipefy, '303509035', cliente, atm_fin)
        
        id_card_eng= infos_card_eng[0]
        info_card_eng = infos_card_eng[1]

        if str(info_card_eng) != 'Não encontrado':

            planilha_atualizada = info_card_eng.loc[info_card_eng['name'] == 'Upload'].iloc[0,1]
            
            planilha_atualizada = requests.get(planilha_atualizada)
            planilha_atualizada = pd.read_excel(planilha_atualizada.content, header=1)
            planilha_atualizada = planilha_atualizada.fillna('')

            atd_itens = str(planilha_atualizada.iloc[0,1])
            pv = str(planilha_atualizada.iloc[0,0])

            planilha_atualizada = planilha_atualizada.iloc[2:,]

            planilha_atualizada.columns = planilha_atualizada.iloc[0]
            planilha_atualizada = planilha_atualizada.iloc[1:,]

            payload_limpa_card_eng = {"query":"mutation{updateFieldsValues(input:\
                                                    {nodeId:\""+str(id_card_eng)+"\", values: [\
                                                    {fieldId: \"lista_atualizada_para_o_depto_de_engenharia\" ,value: null},\
                                                    {fieldId: \"lista_sem_altera_es_para_o_depto_de_engenharia\" ,value: null},\
                                                    {fieldId: \"enviar\" ,value: null}\
                                                    ]}){success}}"}
            
            request_limpa_card_eng = requests.request("POST", url_pipefy, json = payload_limpa_card_eng, headers = headers_pipefy)
            request_limpa_card_eng = json.loads(request_limpa_card_eng.content)

            mover_card(id_card_eng, '321493886')

            nome_arquivo_eng = "lista_pv_"+str(pv)+".xlsx"
            caminho_arquivo = "//192.168.1.16/02 - Público/09 - Marketing/Thales/listas_pvs/"

            nova_planilha = comparar_lista_com_pv(pv, atd_itens, planilha_atualizada)

            envia_arquivo_pipe_projetos(nome_arquivo_eng, caminho_arquivo, id_card_eng)

            infos_card_log = buscar_card(url_pipefy, headers_pipefy, '302451727', cliente, atm_fin) 
            id_card_log = infos_card_log[0]
            info_card_log = infos_card_log[1]

            if str(info_card_log) != 'Não encontrado':
                envia_arquivo_pipe_logistica(nome_arquivo_eng, caminho_arquivo, id_card_log)

            infos_card_pcp = buscar_card(url_pipefy, headers_pipefy, '303442939', cliente, atm_fin) 
            id_card_pcp = infos_card_pcp[0]
            info_card_pcp = infos_card_pcp[1]

            if str(info_card_pcp) != 'Não encontrado':
                envia_arquivo_pipe_pcp(nome_arquivo_eng, caminho_arquivo, id_card_pcp)
            
            update_atualizacao_realizada = "UPDATE SC5010 SET C5_ZATU_LI = 'CONC_ENG' WHERE C5_NUM = '"+str(pv)+"'"
            conn.execute(update_atualizacao_realizada)
        
        linha = linha+ 1
fluxo_lista_eng()

def fluxo_lista_logistica():
    
    #### Lista atualizada Logistica
    pedidos_lista_atualizadas_log = "SELECT SA1.A1_NOME , C5_ZATU_LI, SUA.UA_ZATMFIN, SUA.UA_ZPEDITENS, SUA.UA_NUM FROM SC5010 SC5\
                                    LEFT JOIN SUA010 SUA ON SUA.D_E_L_E_T_ = '' AND SUA.UA_ZPEDITENS = SC5.C5_NUM\
                                    LEFT JOIN SA1010 SA1 ON SA1.D_E_L_E_T_ = '' AND SA1.A1_COD = SUA.UA_CLIENTE\
                                    WHERE C5_ZATU_LI = 'ATU_LOG'"
    pedidos_lista_atualizadas_log = pd.read_sql(pedidos_lista_atualizadas_log, conn)
    print('pedidos_lista_atualizadas_log', pedidos_lista_atualizadas_log)

    linha = 0
    for atm_fin in pedidos_lista_atualizadas_log['UA_ZATMFIN']:
        atd_itens = pedidos_lista_atualizadas_log.iloc[linha, 4]
        cliente = pedidos_lista_atualizadas_log.iloc[linha,0]

        infos_card_log = buscar_card(url_pipefy, headers_pipefy, '302451727', cliente, atm_fin)
        print('infos_card_log', infos_card_log)
        
        id_card_log = infos_card_log[0]
        info_card_log = infos_card_log[1]
        
        if str(info_card_log) != 'Não encontrado':
            planilha_atualizada_log = info_card_log.loc[info_card_log['name'] == 'Lista atualizada Logística'].iloc[0,1]
            
            planilha_atualizada_log = requests.get(planilha_atualizada_log)
            planilha_atualizada_log = pd.read_excel(planilha_atualizada_log.content, header=1)
            planilha_atualizada_log = planilha_atualizada_log.fillna('')

            atd_itens = str(planilha_atualizada_log.iloc[0,1])
            pv = str(planilha_atualizada_log.iloc[0,0])

            planilha_atualizada_log = planilha_atualizada_log.iloc[2:,]

            planilha_atualizada_log.columns = planilha_atualizada_log.iloc[0]
            planilha_atualizada_log = planilha_atualizada_log.iloc[1:,]

            payload_limpa_card_log = {"query":"mutation{updateFieldsValues(input:\
                                                    {nodeId:\""+str(id_card_log)+"\", values: [\
                                                    {fieldId: \"lista_log_stica\" ,value: null}\
                                                    ]}){success}}"}
            
            request_limpa_card_log = requests.request("POST", url_pipefy, json = payload_limpa_card_log, headers = headers_pipefy)
            request_limpa_card_log = json.loads(request_limpa_card_log.content)

            nome_arquivo_log = "lista_pv_"+str(pv)+".xlsx"
            caminho_arquivo = "//192.168.1.16/02 - Público/09 - Marketing/Thales/listas_pvs/"

            nova_planilha = comparar_lista_com_pv(pv, atd_itens, planilha_atualizada_log)

            envia_arquivo_pipe_logistica(nome_arquivo_log, caminho_arquivo, id_card_log)

            infos_card_eng = buscar_card(url_pipefy, headers_pipefy, '303509035', cliente, atm_fin)
            id_card_eng = infos_card_eng[0]
            info_card_eng = infos_card_eng[1]

            if str(info_card_eng) != 'Não encontrado':
                envia_arquivo_pipe_projetos(nome_arquivo_log, caminho_arquivo, id_card_eng)

            infos_card_pcp = buscar_card(url_pipefy, headers_pipefy, '303442939', cliente, atm_fin) 
            id_card_pcp = infos_card_pcp[0]
            info_card_pcp = infos_card_pcp[1]

            if str(info_card_pcp) != 'Não encontrado':
                envia_arquivo_pipe_pcp(nome_arquivo_log, caminho_arquivo, id_card_pcp)
            
            update_atualizacao_realizada = "UPDATE SC5010 SET C5_ZATU_LI = 'CONC_LOG' WHERE C5_NUM = '"+str(pv)+"'"
            conn.execute(update_atualizacao_realizada)
        
        linha = linha+ 1
fluxo_lista_logistica()

def fluxo_lista_logistica_concluido():
    ####  acertar fluxo no make para não pegar quando for vazio
    pedidos_concluido_log = "SELECT SA1.A1_NOME , C5_ZATU_LI, SUA.UA_ZATMFIN, SUA.UA_ZPEDITENS, SUA.UA_NUM FROM SC5010 SC5\
                                    LEFT JOIN SUA010 SUA ON SUA.D_E_L_E_T_ = '' AND SUA.UA_ZPEDITENS = SC5.C5_NUM\
                                    LEFT JOIN SA1010 SA1 ON SA1.D_E_L_E_T_ = '' AND SA1.A1_COD = SUA.UA_CLIENTE\
                                    WHERE C5_ZATU_LI = 'FASE_CONC'"
    pedidos_concluido_log = pd.read_sql(pedidos_concluido_log, conn)
    print('pedidos_lista_atualizadas_log', pedidos_concluido_log)

    linha = 0
    for atm_fin in pedidos_concluido_log['UA_ZATMFIN']:
        atd_itens = pedidos_concluido_log.iloc[linha, 4]
        cliente = pedidos_concluido_log.iloc[linha,0]

        infos_card_conc_log = buscar_card(url_pipefy, headers_pipefy, '302451727', cliente, atm_fin)
        print('infos_card_log', infos_card_conc_log)
        
        id_card_conc_log = infos_card_conc_log[0]
        info_card_conc_log = infos_card_conc_log[1]
        
        if str(info_card_conc_log) != 'Não encontrado':
            planilha_atualizada_conc_log = info_card_conc_log.loc[info_card_conc_log['name'] == 'Lista atualizada Logística'].iloc[0,1]
            
            planilha_atualizada_conc_log = requests.get(planilha_atualizada_conc_log)
            planilha_atualizada_conc_log = pd.read_excel(planilha_atualizada_conc_log.content, header=1)
            planilha_atualizada_conc_log = planilha_atualizada_conc_log.fillna('')

            atd_itens = str(planilha_atualizada_conc_log.iloc[0,1])
            pv = str(planilha_atualizada_conc_log.iloc[0,0])

            planilha_atualizada_conc_log = planilha_atualizada_conc_log.iloc[2:,]

            planilha_atualizada_conc_log.columns = planilha_atualizada_conc_log.iloc[0]
            planilha_atualizada_conc_log = planilha_atualizada_conc_log.iloc[1:,]

            nome_arquivo_eng = "lista_pv_"+str(pv)+".xlsx"
            caminho_arquivo = "//192.168.1.16/02 - Público/09 - Marketing/Thales/listas_pvs/"
            planilha_atualizada_conc_log.to_excel(str(caminho_arquivo)+str(nome_arquivo_eng), index=False)

            planilha_atualizada_conc_log = pd.ExcelFile(planilha_atualizada_conc_log)

            # Abrir o arquivo Excel usando o pandas
            excel_writer = pd.ExcelWriter(planilha_atualizada_conc_log, engine='openpyxl')
            excel_writer.book = Workbook()

            nome_aba = 'entrega'+str(datetime.today()).replace(' ','').replace('-','').replace(':','')
            # Adicionar a nova aba ao arquivo Excel
            info_card_conc_log.to_excel(excel_writer, sheet_name=nome_aba, index=False)

            # Salvar as alterações e fechar o escritor
            excel_writer.save()
            excel_writer.close()

            #### ID FASE PROGRAMAÇÃO LOGISTICA
            mover_card(id_card_conc_log, id_fase_destino)

            ##### APAGAR TODOS OS CAMPOS CARD LOGISTICA - adicionar todos os campos

            payload_limpa_card_log = {"query":"mutation{updateFieldsValues(input:\
                                                    {nodeId:\""+str(id_card_conc_log)+"\", values: [\
                                                    {fieldId: \"lista_atualizada_para_o_depto_de_engenharia\" ,value: null},\
                                                    {fieldId: \"lista_sem_altera_es_para_o_depto_de_engenharia\" ,value: null},\
                                                    {fieldId: \"enviar\" ,value: null}\
                                                    ]}){success}}"}
            
            request_limpa_card_log = requests.request("POST", url_pipefy, json = payload_limpa_card_log, headers = headers_pipefy)
            request_limpa_card_eng = json.loads(request_limpa_card_log.content)

            envia_arquivo_pipe_logistica(info_card_conc_log, caminho_arquivo, id_card_conc_log)

            infos_card_eng = buscar_card(url_pipefy, headers_pipefy, '303509035', cliente, atm_fin)
            id_card_eng = infos_card_eng[0]
            info_card_eng = infos_card_eng[1]

            if str(info_card_eng) != 'Não encontrado':
                envia_arquivo_pipe_projetos(info_card_conc_log, caminho_arquivo, id_card_eng)

            infos_card_pcp = buscar_card(url_pipefy, headers_pipefy, '303442939', cliente, atm_fin) 
            id_card_pcp = infos_card_pcp[0]
            info_card_pcp = infos_card_pcp[1]

            if str(info_card_pcp) != 'Não encontrado':
                envia_arquivo_pipe_pcp(info_card_conc_log, caminho_arquivo, id_card_pcp)
            
            info_card_compras = buscar_card(url_pipefy, headers_pipefy, '303509100', cliente, atm_fin)
            id_card_compras = infos_card_pcp[0]
            info_card_compras = infos_card_pcp[1]
            if str(info_card_compras) != 'Não encontrado':
                envia_arquivo_pipe_compras(nome_arquivo_eng, caminho_arquivo, id_card_compras)
            
            update_atualizacao_realizada = "UPDATE SC5010 SET C5_ZATU_LI = 'CONC_LOG' WHERE C5_NUM = '"+str(pv)+"'"
            conn.execute(update_atualizacao_realizada)

def fluxo_lista_atualizacao_pedido_alterado():
    select_data_envio_stamp = "SELECT SC5.C5_NUM, SUA.UA_ZATMFIN, SUA.UA_NUM, DATEADD(HOUR, -3, SC5.S_T_A_M_P_) AS STAMP, SC5.C5_ZINC_LI, SA1.A1_NOME\
                                    FROM SC5010 SC5 WITH (NOLOCK)\
                                    LEFT JOIN SUA010 SUA ON SUA.D_E_L_E_T_ = '' AND SUA.UA_ZPEDITENS = SC5.C5_NUM\
                                    LEFT JOIN SA1010 SA1 ON SA1.D_E_L_E_T_ = '' AND SA1.A1_COD = SC5.C5_CLIENTE\
                                    WHERE C5_ZINC_LI <> '' AND DATEADD(HOUR, -3, SC5.S_T_A_M_P_) >= DATEADD(MINUTE,3, SC5.C5_ZINC_LI)"
    select_data_envio_stamp = pd.read_sql(select_data_envio_stamp, conn)
    
    linha = 0
    for pedido in select_data_envio_stamp['C5_NUM']:
        atm_fin = select_data_envio_stamp.iloc[linha,1]
        cliente = select_data_envio_stamp.iloc[linha,5]
        infos_card_eng = buscar_card(url_pipefy, headers_pipefy, '303509035', cliente, atm_fin)
                
        id_card_eng = infos_card_eng[0]
        info_card_eng = infos_card_eng[1]

        if str(info_card_eng) != 'Não encontrado':
        
            planilha_atualizada = info_card_eng.loc[info_card_eng['name'] == 'Download'].iloc[0,1]
            
            planilha_atualizada = requests.get(planilha_atualizada)
            planilha_atualizada = pd.read_excel(planilha_atualizada.content, header=1)
            planilha_atualizada = planilha_atualizada.fillna('')

            atd_itens = str(planilha_atualizada.iloc[0,1])
            pv = str(planilha_atualizada.iloc[0,0])

            planilha_atualizada = planilha_atualizada.iloc[2:,]

            planilha_atualizada.columns = planilha_atualizada.iloc[0]
            planilha_atualizada = planilha_atualizada.iloc[1:,]

            payload_limpa_card_eng = {"query":"mutation{updateFieldsValues(input:\
                                                    {nodeId:\""+str(id_card_eng)+"\", values: [\
                                                    {fieldId: \"lista_atualizada_para_o_depto_de_engenharia\" ,value: null},\
                                                    {fieldId: \"lista_sem_altera_es_para_o_depto_de_engenharia\" ,value: null},\
                                                    {fieldId: \"enviar\" ,value: null}\
                                                    ]}){success}}"}
            
            request_limpa_card_eng = requests.request("POST", url_pipefy, json = payload_limpa_card_eng, headers = headers_pipefy)
            request_limpa_card_eng = json.loads(request_limpa_card_eng.content)

            mover_card(id_card_eng, '321493886')

            nome_arquivo_eng = "lista_pv_"+str(pv)+".xlsx"
            caminho_arquivo = "//192.168.1.16/02 - Público/09 - Marketing/Thales/listas_pvs/"

            nova_planilha = comparar_lista_com_pv(pv, atd_itens, planilha_atualizada)

            envia_arquivo_pipe_projetos(nome_arquivo_eng, caminho_arquivo, id_card_eng)
        
        infos_card_log = buscar_card(url_pipefy, headers_pipefy, '302451727', cliente, atm_fin) 
        id_card_log = infos_card_log[0]
        info_card_log = infos_card_log[1]

        if str(info_card_log) != 'Não encontrado' and str(info_card_eng) != 'Não encontrado':
            envia_arquivo_pipe_logistica(nome_arquivo_eng, caminho_arquivo, id_card_log)

        infos_card_pcp = buscar_card(url_pipefy, headers_pipefy, '303442939', cliente, atm_fin) 
        id_card_pcp = infos_card_pcp[0]
        info_card_pcp = infos_card_pcp[1]

        if str(info_card_pcp) != 'Não encontrado' and str(info_card_eng) != 'Não encontrado':
            envia_arquivo_pipe_pcp(nome_arquivo_eng, caminho_arquivo, id_card_pcp)

        data_agora = datetime.today()
        data_agora = data_agora.strftime("%Y-%m-%d %H:%M")
        
        update_data_atualizacao_lista = "UPDATE SC5010 SET C5_ZINC_LI = '"+str(data_agora)+"' WHERE C5_NUM = '"+str(pedido)+"' AND C5_FILIAL = '0101'"
fluxo_lista_atualizacao_pedido_alterado()

