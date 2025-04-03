# -*- coding: utf-8 -*-
"""
Editor Spyder

Este é um arquivo de script temporário.
"""

#%% Agente de PO

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import calendar
import time
from io import BytesIO
import teradatasql
import os
import json
import re
import openai
import urllib3
from dotenv import load_dotenv
import numpy as np


st.set_page_config(layout = 'wide')

st.title('DASHBOARD SUPRIMENTOS :shopping_trolley:')

def formata_numero(valor, prefixo=''):
    for unidade in ['', 'mil', 'milhões', 'bilhões']:
        if valor < 1000:
            return f'{prefixo} {valor:.2f} {unidade}'.strip()
        valor /= 1000
    return f'{prefixo} {valor:.2f} trilhões'

def formata_numero2(valor, prefixo=''):
    for unidade in ['', 'mil', 'MM', 'BI']:
        if valor < 1000:
            return f'{prefixo} {valor:.2f} {unidade}'.strip()
        valor /= 1000
    return f'{prefixo} {valor:.2f} TRI'

def converte_csv(df):
    return df.to_csv(index = False).encode('utf-8')

def mensagem_sucesso():
    sucesso = st.success('Arquivo baixado com sucesso!', icon = "✅" )
    time.sleep(5)
    sucesso.empty()

def converte_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Dados')  # Exportar para Excel sem índice
    return output.getvalue()  # Retorna os bytes do arquivo

def salvar_em_arquivos_csv(df, caminho_base="C:/Users/cvieira/Desktop/Claudio/Area de Trabalho/Dashboards/Dashboards/base_pedidos", tamanho_maximo_mb=22):
    # Garantir que o diretório exista
    diretorio = os.path.dirname(caminho_base)
    os.makedirs(diretorio, exist_ok=True)

    # Converter o tamanho máximo para bytes
    tamanho_maximo_bytes = tamanho_maximo_mb * 1024 * 1024

    # Inicializar variáveis
    arquivo_indice = 1
    linhas_por_teste = 100000  # Número de linhas a serem testadas a cada iteração

    while not df.empty:
        temp_path = f"{caminho_base}{arquivo_indice}.csv"

        # Salvar um subconjunto do DataFrame temporariamente para verificar o tamanho do arquivo
        df.head(linhas_por_teste).to_csv(temp_path, index=False, encoding='utf-8')

        # Verificar o tamanho do arquivo gerado
        if os.path.getsize(temp_path) > tamanho_maximo_bytes:
            # Se o arquivo for muito grande, reduzir o número de linhas por teste
            linhas_por_teste = max(1, int(linhas_por_teste * 0.9))
        else:
            # Salvar o arquivo final e remover as linhas salvas do DataFrame
            print(f"Arquivo gerado: {temp_path}")
            df = df.iloc[linhas_por_teste:]
            arquivo_indice += 1


# Criando as Bases dos Pedidos de Compras do Teratada

base_pedidos = []

# Função para desativar avisos SSL e carregar variáveis de ambiente
def configurar_ambiente():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    load_dotenv()
    global API_KEY
    API_KEY = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY"))
    openai.api_key = API_KEY
    
@st.cache_data(show_spinner="🔍 Trazendo dados do Teradata...")
def executar_teradata():
    
    # Credenciais de conexão
    HOST = "20.185.82.103"
    USER = "cvieira@prio3.com.br"
    PASSWORD = st.secrets["DB_PASSWORD"]
    PASSWORD = st.secrets.get("DB_PASSWORD", os.getenv("DB_PASSWORD"))
    SCHEMA = "AA_PRD_DDM"
    SCHEMA2 = "AA_PRD_WRK"

    # Conectando ao banco de dados Teradata
    try:
        with teradatasql.connect(host=HOST, user=USER, password=PASSWORD) as conn:
            print("Conexão bem-sucedida!")
            
            with conn.cursor() as cur:
                
                # 🔹 Carregar base ZMM0018_PURCHASE_ORDER
                query_zmm0018 = f"""
                    SELECT 
                        "Código Empresa Requisição" AS "Empresa", 
                        "Descrição Empresa Requisição" AS "Nome_Empresa", 
                        "Código Centro Requisição" AS "Planta", 
                        "Descrição Centro Requisição" AS "Nome_Planta", 
                        "Código Tipo de Pedido" AS "Tipo_PO", 
                        "Descrição Tipo de Pedido" AS "Nome_Tipo_PO", 
                        "Número do Pedido" AS "Numero_PO", 
                        "Item do Pedido" AS "Item_PO", 
                        "Ctg.class.cont." AS "Tipo_Contabil", 
                        "Material Pedido" AS "Numero_Material", 
                        "Texto Breve Pedido" AS "Nome_Material", 
                        "Valor Unitário Pedido" AS "Valor_Item", 
                        "Valor total Pedido" AS "Valor_PO", 
                        "Moeda Pedido" AS "Moeda_PO", 
                        "Taxa de câmbio" AS "Cambio_PO", 
                        "Data do Documento Pedido" AS "Data_Pedido", 
                        "Data de Aprovação Pedido" AS "Data_Aprovacao_PO", 
                        "Data de Remessa Pedido" AS "Data_Entrega", 
                        "Quantidade Solicitada Pedido" AS "Quantidade", 
                        "Código Fornecedor Pedido" AS "Numero_Fornecedor", 
                        "Descrição Fornecedor Pedido" AS "Nome_Fornecedor", 
                        "Grupo de Compradores Pedido" AS "Numero_Grupo_Compras", 
                        "Descrição Grupo de Compradores Pedido" AS "Nome_Grupo_Compras", 
                        "Nome do Requisitante Requisição" AS "Nome_Requisitante", 
                        "Data de Recebimento" AS "Entregue?", 
                        "Número do Contrato" AS "Numero_Contrato", 
                        "Número de nota fiscal eletrônica" AS "Numero_NF", 
                        "Data da MIGO" AS "Data_MIGO", 
                        "Data da NF" AS "Data_NF", 
                        "Data da MIRO" AS "Data_MIRO", 
                        "Data do vencimento" AS "Data_Vencimento", 
                        "Centro custo" AS "Centro_Custo", 
                        "Ordem" AS "Ordem_Interna",
                        "FlagExcluidoPedido" AS "Deletado",
                        "MIGO" AS "MIGO",
                        "MIRO" AS "MIRO",
                        "Grupo de Mercadorias Contrato" AS "Numero_Grupo_Material",
                        "Denominação do Grupo de Mercadorias Cont" AS "Nome_Grupo_Material"
                    FROM {SCHEMA}.ZMM0018_PEDIDOS_RELATORIO
                """
                cur.execute(query_zmm0018)
                columns_zmm0018 = [desc[0] for desc in cur.description]
                df_zmm0018 = pd.DataFrame(cur.fetchall(), columns=columns_zmm0018)
                
                # Dicionário de mapeamento dos valores
                mapeamento_tipo_contabil = {
                    "K": "Custo - Opex",
                    "F": "Ordem Interna",
                    "A": "Ativo Imobilizado",
                    "U": "Desconhecido",
                    "": "Estoque",  # Substituir valores vazios
                    np.nan: "Estoque"  # Substituir valores NaN
                }
                
                # Filtrar pedidos não deletados
                df_zmm0018 = df_zmm0018[df_zmm0018["Deletado"].isna() | (df_zmm0018["Deletado"] == "")]
                df_zmm0018 = df_zmm0018.drop(columns=["Deletado"])
                
                # Conversão de valores binários para "Sim" e "Não"
                df_zmm0018["Entregue?"] = df_zmm0018["Entregue?"].astype(str).replace({"0.0": "Não", "1.0": "Sim"})
                
                # Substituição de valores vazios ou nulos por valores padrão
                df_zmm0018["Numero_Contrato"] = df_zmm0018["Numero_Contrato"].replace({"": "Sem Contrato"})
                df_zmm0018["Numero_NF"] = df_zmm0018["Numero_NF"].fillna("Sem NF")
                df_zmm0018["MIGO"] = df_zmm0018["MIGO"].fillna("Sem MIGO")
                df_zmm0018["MIRO"] = df_zmm0018["MIRO"].fillna("Sem MIRO")
                df_zmm0018["Centro_Custo"] = df_zmm0018["Centro_Custo"].fillna("Não Informado")
                df_zmm0018["Centro_Custo"] = df_zmm0018["Centro_Custo"].replace({"":"Não Informado"})
                df_zmm0018["Ordem_Interna"] = df_zmm0018["Ordem_Interna"].fillna("Não Informado")
                
                # Ajuste do campo "Acao"
                df_zmm0018["Tipo_Contabil"] =  df_zmm0018["Tipo_Contabil"].replace(mapeamento_tipo_contabil)
                
                #Ajuste da base
                df_zmm0018 = df_zmm0018[~df_zmm0018['Data_Aprovacao_PO'].isna()]
                df_zmm0018['Valor_Item'] = df_zmm0018['Valor_Item'].astype('float')
                df_zmm0018['Valor_PO'] = df_zmm0018['Valor_PO'].astype('float')
                df_zmm0018['Cambio_PO'] = df_zmm0018['Cambio_PO'].astype('float')
                
                df_zmm0018['Data_MIGO'] = df_zmm0018['Data_MIGO'].fillna("1900-01-01")
                df_zmm0018['Data_NF'] = df_zmm0018['Data_NF'].fillna("1900-01-01")
                df_zmm0018['Data_MIRO'] = df_zmm0018['Data_MIRO'].fillna("1900-01-01")
                df_zmm0018['Data_Vencimento'] = df_zmm0018['Data_Vencimento'].fillna("1900-01-01")
                
                df_zmm0018['Nome_Empresa'] = df_zmm0018['Nome_Empresa'].fillna(df_zmm0018['Nome_Planta'])
                df_zmm0018['Numero_Fornecedor'] = df_zmm0018['Numero_Fornecedor'].fillna('Não Informado')
                df_zmm0018['Nome_Fornecedor'] = df_zmm0018['Nome_Fornecedor'].fillna('Não Informado')
                df_zmm0018['Numero_Grupo_Compras'] = df_zmm0018['Numero_Grupo_Compras'].fillna('Não Informado')
                df_zmm0018['Nome_Grupo_Compras'] = df_zmm0018['Nome_Grupo_Compras'].fillna('Não Informado')
                
                df_zmm0018['Data_Pedido'] = pd.to_datetime(df_zmm0018['Data_Pedido'], errors='coerce', format='%d/%m/%Y')
                df_zmm0018['Data_Aprovacao_PO'] = pd.to_datetime(df_zmm0018['Data_Aprovacao_PO'], errors='coerce', format='%d/%m/%Y')
                df_zmm0018['Data_Entrega'] = pd.to_datetime(df_zmm0018['Data_Entrega'], errors='coerce', format='%d/%m/%Y')
                df_zmm0018['Data_MIGO'] = pd.to_datetime(df_zmm0018['Data_MIGO'], errors='coerce', format='%d/%m/%Y')
                df_zmm0018['Data_NF'] = pd.to_datetime(df_zmm0018['Data_NF'], errors='coerce', format='%d/%m/%Y')
                df_zmm0018['Data_MIRO'] = pd.to_datetime(df_zmm0018['Data_MIRO'], errors='coerce', format='%d/%m/%Y')
                df_zmm0018['Data_Vencimento'] = pd.to_datetime(df_zmm0018['Data_Vencimento'], errors='coerce', format='%d/%m/%Y')
                
                df_zmm0018['Valor_Item_BRL'] = df_zmm0018.apply(
                    lambda row: row['Valor_Item'] * row['Cambio_PO'] if row['Moeda_PO'] != 'BRL' else row['Valor_Item'], axis=1
                )
                
                df_zmm0018['Valor_PO_BRL'] = df_zmm0018.apply(
                    lambda row: row['Valor_PO'] * row['Cambio_PO'] if row['Moeda_PO'] != 'BRL' else row['Valor_PO'], axis=1
                )
                
                df_po = df_zmm0018

                # 🔹 Carregar base PO
        with teradatasql.connect(host=HOST, user=USER, password=PASSWORD) as conn:
            print("Conexão bem-sucedida!")
            
            with conn.cursor() as cur:
                query_po = f"""
                    SELECT PurchaseOrder AS "Numero_PO", PurchaseOrderType AS "Tipo PO", CreatedByUser AS "Criador PO", 
                    PurchasingDocumentDeletionCode AS "Deletado?", PurchasingCompletenessStatus AS "Finalizada?", 
                    PurchasingProcessingStatus AS "Status Processamento", Supplier AS "Numero Fornecedor", ExchangeRate AS "Taxa de Cambio", 
                    LastChangeDateTime AS "Data Final", TaxReturnCountry AS "Moeda PO", PurgReleaseTimeTotalAmount AS "Montante PO", 
                    ZZ1_Aprovador3_PDH AS "Aprovador 3", ZZ1_Aprovador1_PDH AS "Aprovador 1", ZZ1_Aprovador2_PDH AS "Aprovador 2", 
                    ZZ1_DataAprovacaoPO_PDH AS "Data de Aprovacao", ZZ1_Aprovador4_PDH AS "Aprovador 4", PurchaseOrderDate AS "Data do Pedido" 
                    FROM {SCHEMA2}.I_PurchaseOrderAPI01
                """
                cur.execute(query_po)
                columns_po = [desc[0] for desc in cur.description]
                df_aprovador = pd.DataFrame(cur.fetchall(), columns=columns_po)
                
                # 🔹 Processar colunas adicionais na base PO
                df_aprovador["Finalizada?"] = df_aprovador["Finalizada?"].astype(str).replace({"0": "Não", "1": "Sim"})
                df_aprovador["Montante PO"] = df_aprovador["Montante PO"].astype(float)
                df_aprovador["Taxa de Cambio"] = df_aprovador["Taxa de Cambio"].astype(float)
                df_aprovador['Deletado?'] =df_aprovador['Deletado?'].apply(lambda x: x.strip() if isinstance(x, str) else x)
                df_aprovador['Status Processamento'] = df_aprovador['Status Processamento'].apply(lambda x: x.strip() if isinstance(x, str) else x)
                df_aprovador['Tipo PO'] = df_aprovador['Tipo PO'].apply(lambda x: x.strip() if isinstance(x, str) else x)
                
                # 🔹 Criar coluna Aprovador Final
                df_aprovador["Aprovador_Final"] = df_aprovador.apply(
                    lambda row: row["Aprovador 4"] if row["Aprovador 4"] != "N/A" else (
                        row["Aprovador 3"] if row["Aprovador 3"] != "N/A" else (
                            row["Aprovador 2"] if row["Aprovador 2"] != "N/A" else (
                                row["Aprovador 1"] if row["Aprovador 1"] != "N/A" else "Não Aprovado"
                            )
                        )
                    ), axis=1
                )
                
                df_po = df_po.merge(df_aprovador[['Numero_PO','Aprovador_Final']], on="Numero_PO", how="left")
                
                df_po = df_po.dropna(subset=["Valor_PO"])
                
                df_po["Chave_Fornecedor"] = df_po["Numero_Fornecedor"].astype(str) + \
                                df_po["Valor_PO"].astype(int).astype(str) + \
                                df_po["Data_Pedido"].dt.strftime("%m%Y")
               
                # Criar a base Sumario de Pedidos agrupando pela Chave Fornecedor
                df_sumario_pedidos = df_po[["Chave_Fornecedor"]].drop_duplicates()
                
                # Adicionar a coluna Quantidade contando os POs distintos por Chave Fornecedor
                df_sumario_pedidos["Quantidade_POs"] = df_sumario_pedidos["Chave_Fornecedor"].map(
                    df_po.groupby("Chave_Fornecedor")["Numero_PO"].nunique()
                )
                
                # 🔹 Carregar Base Autorizadores
                caminho_autorizadores = "Autorizadores.xlsx"
                df_autorizadores = pd.read_excel(caminho_autorizadores, sheet_name="Autorizadores", header=0)
                df_autorizadores = df_autorizadores.drop(df_autorizadores.columns[[0, 1, -1]], axis=1)
                df_autorizadores.columns = df_autorizadores.iloc[0]
                df_autorizadores = df_autorizadores[1:].reset_index(drop=True)
                df_autorizadores = df_autorizadores.rename(columns={"Uusário SAP": "Aprovador_Final", "Área Autorizador": "Area_Autorizador"})
                df_autorizadores = df_autorizadores[["Aprovador_Final", "Cargo", "Area_Autorizador"]]
                
                # 🔹 Criar as colunas Cargo e Area Autorizador
                df_po = df_po.merge(df_autorizadores[['Aprovador_Final', 'Cargo']], on="Aprovador_Final", how="left")
                df_po = df_po.merge(df_autorizadores[['Aprovador_Final', 'Area_Autorizador']], on="Aprovador_Final", how="left")
                
                # Regra de alçada baseada no dia de virada
                df_po["Data_Aprovacao_PO"] = pd.to_datetime(df_po["Data_Aprovacao_PO"], errors='coerce')
                df_po['Dia de Aprovacao'] = df_po['Data_Aprovacao_PO'].dt.day
                def verificar_alcada(row):
                    if row["Dia de Aprovacao"] < 4578:
                        if (
                            (row["Valor_PO_BRL"] > 100000 and row["Cargo"] == "Diretor") or
                            (row["Valor_PO_BRL"] > 10000 and row["Cargo"] in ["Diretor", "Gerente"]) or
                            (row["Cargo"] in ["Coordenador", "Gerente", "Diretor"])
                        ):
                            return "Efetivo"
                    else:
                        if (
                            (row["Valor_PO_BRL"] > 500000 and row["Cargo"] == "Diretor") or
                            (row["Valor_PO_BRL"] > 15000 and row["Cargo"] in ["Diretor", "Gerente"]) or
                            (row["Cargo"] in ["Coordenador", "Gerente", "Diretor"])
                        ):
                            return "Efetivo"
                    return "Inefetivo"

                # Aplica a função
                df_po["Check Alcada"] = df_po.apply(verificar_alcada, axis=1)
                
                # 🔹 Criar coluna Check Work Days
                df_po["Check Work Days"] = df_po["Data_Aprovacao_PO"].apply(lambda x: "Sim" if x.weekday() >= 5 else "Não")
                
                # 🔹 Criar coluna Check Fornecedor na Base PO
                df_po = df_po.merge(df_sumario_pedidos[["Chave_Fornecedor", "Quantidade_POs"]], on="Chave_Fornecedor", how="left")
                
                df_po["Check Fornecedor"] = df_po["Quantidade_POs"].apply(lambda x: "Sim" if x > 1 else "Não")
                
                # 🔹 Carregar Base Classificação de Risco
                caminho_classificacao_risco = "Base Materiais Compliance.xlsx"
                df_classificacao_risco = pd.read_excel(caminho_classificacao_risco, sheet_name="Classificação de Risco")
                df_classificacao_risco = df_classificacao_risco.iloc[4:]
                df_classificacao_risco.columns = df_classificacao_risco.iloc[0]
                df_classificacao_risco = df_classificacao_risco[1:].reset_index(drop=True)
                df_classificacao_risco.columns = df_classificacao_risco.iloc[0]
                df_classificacao_risco = df_classificacao_risco.iloc[1:]
                df_classificacao_risco = df_classificacao_risco.rename(columns={
                    "TIPO DE FORNECIMENTO": "Tipo de Fornecimento",
                    "GRUPO DE MERCADORIA": "Numero_Grupo_Material",
                    "CATEGORIA": "Categoria",
                    "SUBCATEGORIA": "Subcategoria",
                    "Saúde e SeguranCa": "Saude e Seguranca",
                    "Operacional": "Operacional",
                    "Meio Ambiente": "Meio Ambiente",
                    "COMPLIANCE": "Compliance",
                    "RESULTADO": "Resultado"
                })
                
                # 🔹 Criar Base Sumario Compliance
                df_sumario_compliance = df_po[["Numero_PO", "Numero_Grupo_Material"]].drop_duplicates()
                df_sumario_compliance['Chave Compliance'] = df_sumario_compliance['Numero_PO'] + df_sumario_compliance['Numero_Grupo_Material']
                df_sumario_compliance['Numero_Grupo_Material'] = df_sumario_compliance['Numero_Grupo_Material'].apply(lambda x: x.strip() if isinstance(x, str) else x)
                df_sumario_compliance = df_sumario_compliance.merge(df_classificacao_risco[["Numero_Grupo_Material", "Resultado"]], on="Numero_Grupo_Material", how="left")
                
                # 🔹 Criar Base Sumario de Itens
        with teradatasql.connect(host=HOST, user=USER, password=PASSWORD) as conn:
            print("Conexão bem-sucedida!")
            
            with conn.cursor() as cur:
                
                query_sumario_itens = f"""
                    SELECT DISTINCT "Material Pedido" AS "Numero_Material", "Texto Breve Pedido" AS "Nome_Material"
                    FROM {SCHEMA}.ZMM0018_PEDIDOS_RELATORIO
                    WHERE "FlagExcluidoPedido" = ' '
                """
                cur.execute(query_sumario_itens)
                columns_sumario_itens = [desc[0] for desc in cur.description]
                df_sumario_itens = pd.DataFrame(cur.fetchall(), columns=columns_sumario_itens)
                df_sumario_itens = df_sumario_itens[['Numero_Material']].drop_duplicates()
                
                # 🔹 Calcular Média Geral
                df_media_geral = df_po[df_po["Valor_Item_BRL"] > 100].groupby("Numero_Material", as_index=False)["Valor_Item_BRL"].agg(
                    Media_Geral=lambda x: x.median() if str(x.name).startswith("8") else x.mean()
                )
                df_media_geral.columns = ["Numero_Material", "Media_Geral"]
                
                # 🔹 Adicionar Média Geral ao df_sumario_itens
                df_sumario_itens = df_sumario_itens.merge(df_media_geral, on="Numero_Material", how="left")
                df_sumario_itens['Media_Geral'] = df_sumario_itens['Media_Geral'].fillna(0)

                df_po["Ano"] = pd.to_datetime(df_po["Data_Pedido"]).dt.year
                df_po = df_po.merge(df_sumario_itens[["Numero_Material", "Media_Geral"]],on="Numero_Material", how="left")
                df_po['Preco_Medio'] = df_po['Media_Geral']
                df_po['Dif_Preco'] = df_po['Valor_Item_BRL'] - df_po['Preco_Medio']
                df_po["Check Preco"] = df_po.apply(lambda row: "Sim" if row["Valor_Item_BRL"] > 1.2 * row["Preco_Medio"] and row["Dif_Preco"] > 50000 else "Não", axis=1)
                df_po['Chave Compliance'] = df_po['Numero_PO'] + df_po['Numero_Grupo_Material']
                df_po['Numero_Grupo_Material'] = df_po['Numero_Grupo_Material'].apply(lambda x: x.strip() if isinstance(x, str) else x)
                
                # 🔹 Criar Base Sumario Riscos
                df_sumario_riscos = df_po[["Numero_PO"]].drop_duplicates()

                # 🔹 Criar coluna Risco de Alcada na Base Sumario Riscos
                df_sumario_riscos = df_sumario_riscos.merge(df_po[["Numero_PO", "Check Alcada"]].drop_duplicates(), on="Numero_PO", how="left")
                df_sumario_riscos["Risco de Alcada"] = df_sumario_riscos["Check Alcada"].apply(lambda x: 1 if x == "Inefetivo" else 0)
                
                # Criar a coluna 'Risco de Workdays' na base Sumario Riscos
                df_sumario_riscos = df_sumario_riscos.merge(df_po[['Numero_PO', 'Check Work Days']].drop_duplicates(), on="Numero_PO", how="left")
                df_sumario_riscos["Risco de Workdays"] = df_sumario_riscos["Check Work Days"].apply(lambda x: 1 if x == "Sim" else 0)
                
                # Criar a coluna 'Risco de Fornecedor' na base Sumario Riscos
                df_sumario_riscos = df_sumario_riscos.merge(df_po[['Numero_PO', 'Check Fornecedor']].drop_duplicates(), on="Numero_PO", how="left")
                df_sumario_riscos["Risco de Fornecedor"] = df_sumario_riscos["Check Fornecedor"].apply(lambda x: 2 if x == "Sim" else 0)
                
                # 🔹 Criar coluna Check Compliance
                df_po = df_po.merge(df_sumario_compliance[["Chave Compliance", "Resultado"]].drop_duplicates(), on="Chave Compliance", how="left")
                df_po["Check Compliance"] = df_po["Resultado"].apply(lambda x: "N/A" if pd.isna(x) else x)

                # Criar base compliance alto, médio e baixo
                df_compliance_alto = df_po[df_po['Check Compliance']=='Alta']
                df_compliance_medio = df_po[df_po['Check Compliance']=='Média']
                df_compliance_baixo = df_po[df_po['Check Compliance']=='Baixa']
                
                # 🔹 Criar Base Sumario Preco
                df_sumario_preco = df_po[df_po["Check Preco"] == "Sim"][["Numero_PO"]].drop_duplicates()
                
                # Criar a coluna 'Risco de Preço' na base Sumario Riscos
                df_sumario_riscos = df_sumario_riscos.merge(df_sumario_preco[['Numero_PO']].drop_duplicates(), on="Numero_PO", how="left", indicator=True)
                df_sumario_riscos["Risco de Preco"] = df_sumario_riscos["_merge"].apply(lambda x: 2 if x == "both" else 0)
                df_sumario_riscos.drop(columns=["_merge"], inplace=True)
                
                if not df_compliance_alto.empty:
                    df_sumario_riscos = df_sumario_riscos.merge(df_compliance_alto[['Numero_PO']].drop_duplicates(), 
                                                                on="Numero_PO", how="left", indicator="Compliance_Alto")
                else:
                    df_sumario_riscos["Compliance_Alto"] = "none"
                
                if not df_compliance_medio.empty:
                    df_sumario_riscos = df_sumario_riscos.merge(df_compliance_medio[['Numero_PO']].drop_duplicates(), 
                                                                on="Numero_PO", how="left", indicator="Compliance_Medio")
                else:
                    df_sumario_riscos["Compliance_Medio"] = "none"
                
                if not df_compliance_baixo.empty:
                    df_sumario_riscos = df_sumario_riscos.merge(df_compliance_baixo[['Numero_PO']].drop_duplicates(), 
                                                                on="Numero_PO", how="left", indicator="Compliance_Baixo")
                else:
                    df_sumario_riscos["Compliance_Baixo"] = "none"

                
                # Aplicamos a lógica para definir os níveis de risco
                df_sumario_riscos["Risco de Compliance"] = df_sumario_riscos.apply(
                    lambda row: 3 if row["Compliance_Alto"] == "both" else 
                                2 if row["Compliance_Medio"] == "both" else 
                                1 if row["Compliance_Baixo"] == "both" else 0, axis=1
                )
                
                # Criar um dicionário de mapeamento de 'Numero PO' para 'Check Alcada'
                mapa_alcada = df_po.set_index("Numero_PO")["Check Alcada"].to_dict()
                
                # Criar a coluna "Risco de Alcada" na base df_sumario_riscos
                df_sumario_riscos["Risco de Alcada"] = df_sumario_riscos["Numero_PO"].map(mapa_alcada)
                
                # Converter "Inefetivo" para 1 e os demais valores para 0
                df_sumario_riscos["Risco de Alcada"] = df_sumario_riscos["Risco de Alcada"].apply(lambda x: 1 if x == "Inefetivo" else 0)
                
                # Criar um dicionário de mapeamento de 'Numero PO' para 'Check Work Days'
                mapa_workdays = df_po.set_index("Numero_PO")["Check Work Days"].to_dict()
                
                # Criar a coluna "Risco de Workdays" na base df_sumario_riscos
                df_sumario_riscos["Risco de Workdays"] = df_sumario_riscos["Numero_PO"].map(mapa_workdays)
                
                # Converter "Sim" para 1 e os demais valores para 0
                df_sumario_riscos["Risco de Workdays"] = df_sumario_riscos["Risco de Workdays"].apply(lambda x: 1 if x == "Sim" else 0)

                # Criar um dicionário de mapeamento de 'Numero PO' para 'Check Fornecedor'
                mapa_fornecedor = df_po.set_index("Numero_PO")["Check Fornecedor"].to_dict()
                
                # Criar a coluna "Risco de Fornecedor" na base df_sumario_riscos
                df_sumario_riscos["Risco de Fornecedor"] = df_sumario_riscos["Numero_PO"].map(mapa_fornecedor)
                
                # Converter "Sim" para 2 e os demais valores para 0
                df_sumario_riscos["Risco de Fornecedor"] = df_sumario_riscos["Risco de Fornecedor"].apply(lambda x: 2 if x == "Sim" else 0)
                
                # Criar a coluna 'Risco Geral' na base Sumario Riscos
                df_sumario_riscos["Risco Geral"] = df_sumario_riscos.apply(
                    lambda row: "Alto" if (
                        row["Risco de Alcada"] + row["Risco de Compliance"] + 
                        row["Risco de Fornecedor"] + row["Risco de Preco"] + row["Risco de Workdays"] >= 5
                    ) else "Moderado" if (
                        row["Risco de Alcada"] + row["Risco de Compliance"] +  
                        row["Risco de Fornecedor"] + row["Risco de Preco"] + row["Risco de Workdays"] >= 3
                    ) else "Baixo" if (
                        row["Risco de Alcada"] + row["Risco de Compliance"] +  
                        row["Risco de Fornecedor"] + row["Risco de Preco"] + row["Risco de Workdays"] >= 1
                    ) else "Desprezível",
                    axis=1
                )
                
                # Definir ordem de prioridade para os níveis de risco
                prioridade_risco = {"Alto": 3, "Moderado": 2, "Baixo": 1, "Desprezível": 0}
                
                # Criar função personalizada para selecionar o risco com maior prioridade
                def escolher_risco_mais_alto(riscos):
                    return max(riscos, key=lambda x: prioridade_risco.get(x, -1))  # Se não encontrar, assume -1
                
                # Aplicar groupby e agregação
                df_sumario_riscos = df_sumario_riscos.groupby("Numero_PO", as_index=False).agg({
                    "Risco de Compliance": "max",  # Pega o maior nível de risco
                    "Risco de Alcada": "max",      # Mantém o maior risco de alçada
                    "Risco de Workdays": "max",    # Mantém o maior risco de workdays
                    "Risco de Fornecedor": "max",  # Mantém o maior risco de fornecedor
                    "Risco de Preco": "max",       # Mantém o maior risco de preço
                    "Risco Geral": escolher_risco_mais_alto  # Usa a função personalizada para priorizar os riscos
                })

                df_po["Check Compliance"] = df_po["Numero_PO"].map(
                    df_sumario_riscos.set_index("Numero_PO")["Risco de Compliance"]
                )
             
                # Substituir valores numéricos por rótulos descritivos
                df_po["Check Compliance"] = df_po["Check Compliance"].replace({
                    3: "Alto",
                    2: "Médio",
                    1: "Baixo",
                    0: "Desprezível"
                }).fillna("N/A")

                # Criando a base_pedidos a partir das colunas necessárias
                base_pedidos = df_po.copy()
                
                # Adicionando informações de Risco Geral da Base Sumario de Riscos
                base_pedidos = base_pedidos.merge(df_sumario_riscos[["Numero_PO", "Risco Geral"]], 
                                                  on="Numero_PO", how="left")
                
                # Renomeando colunas conforme solicitado
                base_pedidos.rename(columns={"Valor_PO_BRL": "Valor PO - R$", 
                                             "Entregue?": "Entregue"}, inplace=True)
                base_pedidos['Tipo_Contabil'] = base_pedidos['Tipo_Contabil'].apply(lambda x: x.strip() if isinstance(x, str) else x)
                
                # Aplicar a substituição
                base_pedidos["Data_Pedido"] = pd.to_datetime(base_pedidos["Data_Pedido"], errors='coerce')
                base_pedidos["Data_Pedido"] = base_pedidos["Data_Pedido"].apply(
                    lambda x: x.replace(year=2022) if x.year == 2202 else x
                )
                base_pedidos = base_pedidos.dropna(subset=['Data_Aprovacao_PO'])
                
                base_pedidos = base_pedidos.rename(columns={
                    'Numero_PO':'Numero PO',
                    'Data_Pedido':'Data do Pedido',
                    'Tipo_PO':'Tipo PO',
                    'Tipo_Contabil':'Tipo Contabil',
                    'Aprovador_Final':'Aprovador Final',
                    'Data_Aprovacao_PO':'Data de Aprovacao',
                    'Numero_Fornecedor':'Numero Fornecedor',
                    'Nome_Fornecedor':'Nome Fornecedor',
                    'Area_Autorizador': 'Area Autorizador'})
            
                converte_csv(base_pedidos)
                
                
                return df_po, base_pedidos, df_zmm0018
                
                
    #except Exception as e:
       # print("Erro na conexão:", e)
       # return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    except Exception as e:
        st.error(f"❌ Erro na conexão com Teradata: {e}")
        st.stop()
    
#Executar Funções de Configuração

configurar_ambiente()
df_po, base_pedidos, df_zmm0018 = executar_teradata()

if base_pedidos.empty:
    st.error("❌ Nenhum dado foi carregado da base de pedidos. Verifique as credenciais ou a conexão com o banco.")
    st.stop()

#Criar Bancos de Dados para o Streamlit
df = pd.DataFrame(base_pedidos)
df.columns = [str(col).strip() for col in df.columns]  # Remove espaços no início e no fim
df.columns = df.columns.astype(str).str.strip()
df.columns = df.columns.str.replace('–', '-')  # Substitui EN DASH por hífen normal
df.columns = df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')  # Remove acentos e caracteres especiais
df['Data do Pedido'] = pd.to_datetime(df['Data do Pedido'], errors = 'coerce')
df.rename(columns={'Valor PO - R$': 'Valor_PO_BRL', 'Valor_Item_BRL': 'Valor'}, inplace=True)
data_min = df['Data do Pedido'].min().strftime('%d/%m/%Y')  # Formata como dd/mm/yyyy
data_max = df['Data do Pedido'].max().strftime('%d/%m/%Y')  # Formata como dd/mm/yyyy
df['Ano'] = df['Data do Pedido'].dt.year

# Sidebar - Filtros
st.sidebar.title('Filtros')

opcoes_fornecedor = ["Todos"] + list(df['Nome Fornecedor'].dropna().unique())
opcoes_area = ["Todos"] + list(df['Area Autorizador'].dropna().unique())
opcoes_contabil = ["Todos"] + list(df['Tipo Contabil'].dropna().unique())
opcoes_ano = ['Todos'] + [2025,2024,2023,2022]
lista_aprovadores = ['Todos'] + sorted(df['Aprovador Final'].unique(), reverse=False)

with st.sidebar.expander('Selecione o Fornecedor'):
    fornecedor = st.selectbox('Fornecedores', opcoes_fornecedor, index=0)
    
with st.sidebar.expander('Selecione a Área'):
    area = st.selectbox('Áreas', opcoes_area, index=0, key='selecionar area')
    
with st.sidebar.expander('Aprovador'):
    aprovador = st.selectbox('Selecione o aprovador', lista_aprovadores, index=0)
    
with st.sidebar.expander('Seleciaone a Categoria'):
    tipo_contabil = st.selectbox('Tipo Contábil', opcoes_contabil, index=0)

with st.sidebar.expander('Selecione o Ano'):
    ano_escolhido = st.multiselect('Anos', opcoes_ano,default=[2024,2025], key='selecionar ano')
    

data_pedido = st.sidebar.slider(
    'Selecione a Data',
    min_value=df['Data do Pedido'].min().date(),  # Converte para apenas a data
    max_value=df['Data do Pedido'].max().date(),  # Converte para apenas a data
    value=(df['Data do Pedido'].min().date(), df['Data do Pedido'].max().date()),  # Intervalo inicial
    format="DD/MM/YYYY",  # Formato brasileiro de data
    key='data_pedido'
)

if fornecedor != "Todos":
    df = df[df['Nome Fornecedor'] == fornecedor]

if area != "Todos":
    df = df[df['Area Autorizador'] == area]

if tipo_contabil != "Todos":
    df = df[df['Tipo Contabil'] == tipo_contabil]
    
if ano_escolhido != 'Todos':
    df = df[df['Ano'].isin(ano_escolhido)]

if aprovador != 'Todos':
    df = df[df['Aprovador Final'] == aprovador]
    
# Verificar se o usuário deixou o filtro vazio ou inválido
if not data_pedido or len(data_pedido) != 2 or data_pedido[0] > data_pedido[1]:
    st.sidebar.warning("Nenhuma data válida foi selecionada. Exibindo todos os dados.")
else:
    # Aplicar filtro de data apenas se válido
    df = df[
        (df['Data do Pedido'] >= pd.to_datetime(data_pedido[0])) &
        (df['Data do Pedido'] <= pd.to_datetime(data_pedido[1]))
    ]

# Tabelas

pedidos_fornecedor = df.groupby(['Ano', 'Nome Fornecedor'])['Valor'].agg(['sum', 'count']) \
    .rename(columns={'sum': 'Valor Total', 'count': 'Quantidade'}) \
    .sort_values(by=['Ano', 'Valor Total'], ascending=[True, False]) \
    .reset_index()
pedidos_fornecedor['Valor Total Formatado'] = pedidos_fornecedor['Valor Total'].apply(lambda x: formata_numero2(x, 'R$'))
pedidos_fornecedor['Ano'] = pedidos_fornecedor['Ano'].astype(str)

pedidos_chat = df[['Ano','Numero PO', 'Nome Fornecedor', 'Valor', 'Numero_Contrato', 'Nome_Material', 'Area Autorizador', 'Quantidade']]
pedidos_chat['Quantidade'] = pedidos_chat['Quantidade'].astype('float')
pedidos_chat['Quantidade'] = pedidos_chat['Quantidade']
pedidos_chat.info()

pedidos_mensal = df.copy()
pedidos_mensal['Ano'] = pedidos_mensal['Data do Pedido'].dt.year
pedidos_mensal['Mes'] = pedidos_mensal['Data do Pedido'].dt.month.apply(lambda x: calendar.month_name[x])
pedidos_mensal = pedidos_mensal.groupby(['Ano', 'Mes'], as_index=False)['Valor'].agg('sum')
meses_ordem = list(calendar.month_name[1:])
pedidos_mensal['Mes'] = pd.Categorical(pedidos_mensal['Mes'], categories=meses_ordem, ordered=True)
pedidos_mensal = pedidos_mensal.sort_values(['Ano', 'Mes']).reset_index(drop=True)
pedidos_mensal['Valor Formatado'] = pedidos_mensal['Valor'].apply(lambda x: formata_numero2(x, 'R$'))

pedidos_area = df.groupby(['Ano', 'Area Autorizador'])['Valor'].agg('sum').reset_index()
pedidos_area['Valor'] = round(pedidos_area['Valor'], 0)
pedidos_area['Valor Formatado'] = pedidos_area['Valor'].apply(lambda x: formata_numero2(x, 'R$'))
pedidos_area = pedidos_area.sort_values(['Ano', 'Valor'], ascending=[True, False]).reset_index(drop=True)
pedidos_area['Ano'] = pedidos_area['Ano'].astype(str)

df_compliance = df.copy()
df_compliance['Check Compliance'] = df_compliance['Check Compliance'].fillna('Baixo')
df_compliance = df_compliance[df_compliance['Check Compliance'] != 'Desprezível']

pedidos_compliance = df_compliance.groupby('Check Compliance')['Numero PO'].agg('count').reset_index()
pedidos_compliance['Numero Formatado'] = pedidos_compliance['Numero PO'].apply(lambda x: formata_numero2(x))

pedidos_compliance_alto = df_compliance[df_compliance['Check Compliance'].isin(['Alto','Médio'])].groupby(['Ano','Nome Fornecedor'])['Valor'].agg('sum').sort_values(ascending=False).reset_index()
pedidos_compliance_alto['Valor Formatado'] = pedidos_compliance_alto['Valor'].apply(lambda x: formata_numero2(x, 'R$'))
pedidos_compliance_alto['Ano'] = pedidos_compliance_alto['Ano'].astype(str)

pedidos_compliance_alto_area = df_compliance[df_compliance['Check Compliance'] == 'Alto'].groupby(['Ano','Area Autorizador'])['Valor'].agg('sum').sort_values(ascending=False).reset_index()
pedidos_compliance_alto_area['Valor Formatado'] = pedidos_compliance_alto_area['Valor'].apply(lambda x: formata_numero2(x, 'R$'))
pedidos_compliance_alto_area['Ano'] = pedidos_compliance_alto_area['Ano'].astype(str)

# Gráficos
cores = {
    'Alto': '#B22222',    
    'Médio': '#FFD700', 
    'Baixo': '#228B22'   
}

cores2 = {
    'Inefetivo': '#B22222',     
    'Efetivo': '#228B22'   
}

cores3 = {
    '2022':"#94AFC5",
    '2023':"#6495ED",
    '2024':"#174A7E",
    '2025':'#4A81BF'
    }

cores4 = {
    2022 :"#94AFC5",
    2023 :"#6495ED",
    2024 :"#174A7E",
    2025 :'#4A81BF'
    }

cores5 = {
    'Concluído': '#174A7E',
    'Em andamento': '#4A81BF'
    }

## ABA1                 

fig_pedidos_mensal = px.line(pedidos_mensal,
                             x = 'Mes',
                             y = 'Valor',
                             text = 'Valor Formatado',
                             title = '💵 Gastos por mês',
                             color = 'Ano',
                             color_discrete_map = cores4,
                             markers = True)


# Remover os labels dos eixos X e Y

fig_pedidos_mensal.update_layout(
    xaxis_title=None,
    yaxis_title=None,
    yaxis=dict(range=[0, pedidos_mensal['Valor'].max()*1.1],showticklabels=False)
    )

fig_pedidos_mensal.update_traces(textposition='top center', textfont=dict(color='black'))

# Ensure 'dados_carregados' exists in session state before using it
if "dados_carregados" not in st.session_state:
    st.session_state["dados_carregados"] = False  # Initialize as False

st.logo("PRIO_SEM_POLVO_PRIO_PANTONE_LOGOTIPO_Azul.png")
    
aba1, aba2, aba3, aba4 = st.tabs(['Visão Geral dos Pedidos', 'Compliance', 'Busca Agente', 'Base de Dados'])

with aba1:
    st.plotly_chart(fig_pedidos_mensal,use_container_width = True)
    numero_fornecedor = st.number_input('Quantos fornecedores deseja visualizar?',min_value = 1, value = 5, key= 'numero_fornecedor')
    numero_fornecedor = min(numero_fornecedor, len(pedidos_fornecedor))
    
    coluna1, coluna2 = st.columns(2)
    with coluna1:
        st.metric('Gasto Total - R$', formata_numero(df['Valor'].sum(), 'R$'))
        
        top_fornecedores = pedidos_fornecedor.groupby('Nome Fornecedor')['Valor Total'].sum().nlargest(numero_fornecedor).index
        pedidos_fornecedor_filtrados = pedidos_fornecedor[pedidos_fornecedor['Nome Fornecedor'].isin(top_fornecedores)].sort_values(by = 'Valor Total', ascending = False)
        y_max_pedidos_fornecedor = pedidos_fornecedor_filtrados.groupby(['Nome Fornecedor', 'Ano'])['Valor Total'].sum().max() * 1.4
    
        fig_pedidos_fornecedor = px.bar(
            pedidos_fornecedor_filtrados,
            x='Nome Fornecedor',
            y='Valor Total',  # O eixo Y mantém os valores numéricos para não afetar a escala
            text='Valor Total Formatado',  # Exibir valores formatados nos rótulos
            title=f'Gastos pelo Top {numero_fornecedor}',
            color = 'Ano',
            color_discrete_map = cores3,
            barmode='stack'
        )
        
        fig_pedidos_fornecedor.update_layout(
            xaxis_title=None,
            yaxis_title=None
        )

        fig_pedidos_fornecedor.update_yaxes(title=None, showticklabels=False, range=[0, y_max_pedidos_fornecedor])
        
        st.plotly_chart(fig_pedidos_fornecedor, use_container_width = True)
        
    with coluna2:
        st.metric('Quantidade de Pedidos', formata_numero(df.shape[0]))
        
        top_areas = pedidos_area.groupby('Area Autorizador')['Valor'].sum().nlargest(numero_fornecedor).index
        pedidos_area_filtrados = pedidos_area[pedidos_area['Area Autorizador'].isin(top_areas)].sort_values(by='Valor', ascending = False)
        y_max_pedidos_area = pedidos_area_filtrados.groupby(['Area Autorizador', 'Ano'])['Valor'].sum().max() * 1.4
        
        fig_pedidos_area = px.bar(
            pedidos_area_filtrados,
            x='Area Autorizador',
            y='Valor',  # Usar a coluna numérica para manter a escala correta
            title=f'Gastos por área (Top {numero_fornecedor})',
            color = 'Ano',
            color_discrete_map = cores3,
            barmode='stack',
            text='Valor Formatado'  # Exibir os valores formatados como rótulos
        )
        
        fig_pedidos_area.update_layout(
            xaxis_title=None,
            yaxis_title=None
        )

        fig_pedidos_area.update_yaxes(title=None, showticklabels=False,range=[0, y_max_pedidos_area])
        
        st.plotly_chart(fig_pedidos_area,  use_container_width = True)
        
        
with aba2: 
    numero_fornecedores = st.number_input('Quantos itens deseja visualizar?',min_value = 1, value = 5, key= 'numero_fornecedores')
    numero_fornecedores = min(numero_fornecedores, len(pedidos_fornecedor))
    
    fig_compliance_riscos = px.bar(pedidos_compliance,
                                   x = 'Check Compliance',
                                   y = 'Numero PO',
                                   text = 'Numero Formatado',
                                   title = 'Distribuição dos pedidos - riscos',
                                   color = 'Check Compliance',
                                   color_discrete_map = cores)
    
    top_fornecedor_alto = pedidos_compliance_alto.groupby('Nome Fornecedor')['Valor'].sum().nlargest(numero_fornecedores).index
    pedidos_compliance_alto_filtrados = pedidos_compliance_alto[pedidos_compliance_alto['Nome Fornecedor'].isin(top_fornecedor_alto)]
    y_max_pedidos_compliance_alto = pedidos_compliance_alto_filtrados.groupby(['Nome Fornecedor', 'Ano'])['Valor'].sum().max() * 1.4

    fig_fornecedor_alto = px.bar(pedidos_compliance_alto_filtrados,
                                 x = 'Nome Fornecedor',
                                 y = 'Valor',
                                 text = 'Valor Formatado',
                                 title = f'Top {numero_fornecedores} fornecedores de risco',
                                 color = 'Ano',
                                 color_discrete_map = cores3,
                                 barmode='stack')
    
    top_fornecedor_alto_area = pedidos_compliance_alto_area.groupby('Area Autorizador')['Valor'].sum().nlargest(numero_fornecedores).index
    pedidos_compliance_alto_area_filtrados = pedidos_compliance_alto_area[pedidos_compliance_alto_area['Area Autorizador'].isin(top_fornecedor_alto_area)]
    y_max_pedidos_compliance_alto_area = pedidos_compliance_alto_area_filtrados.groupby(['Area Autorizador', 'Ano'])['Valor'].sum().max() * 1.4

    fig_fornecedor_alto_area = px.bar(pedidos_compliance_alto_area_filtrados,
                                 x = 'Area Autorizador',
                                 y = 'Valor',
                                 text = 'Valor Formatado',
                                 title = f'Top {numero_fornecedores} áreas de maior risco',
                                 color = 'Ano',
                                 color_discrete_map = cores3,
                                 barmode='stack')
    
    fig_compliance_riscos.update_layout(
        xaxis_title=None,
        yaxis_title=None,
        showlegend=False
    )

    fig_compliance_riscos.update_yaxes(title=None, showticklabels=False)
    
    fig_fornecedor_alto.update_layout(
        xaxis_title=None,
        yaxis_title=None
    )
    
    fig_fornecedor_alto.update_yaxes(title=None, showticklabels=False, range=[0, y_max_pedidos_compliance_alto])

    fig_fornecedor_alto_area.update_layout(
        xaxis_title=None,
        yaxis_title=None
    )
    
    fig_fornecedor_alto_area.update_yaxes(title=None, showticklabels=False, range=[0, y_max_pedidos_compliance_alto_area])
    
    st.plotly_chart(fig_compliance_riscos, use_container_width = True)
    

    st.plotly_chart(fig_fornecedor_alto, use_container_width = True)

    st.plotly_chart(fig_fornecedor_alto_area, use_container_width = True)


with aba3:
    coluna1, coluna2 = st.columns(2)
    
    st.header('Pergunte ao Agente (IA) 🤖')
    
    # Desativa os avisos de SSL inseguros
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Entrada do usuário no chat
    prompt_chat = st.chat_input("Pergunte o que quiser")
    resposta_final = "❌ Nenhuma resposta foi gerada."
    
    if prompt_chat:
        st.write(f"🤔 Você: {prompt_chat}")
        
        headers = {
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json"
                }
        
    # Tenta extrair fornecedor e ano da pergunta
        match = re.search(r"fornecedor (.+?) em (\d{4})", prompt_chat, re.IGNORECASE)
        if match:
            fornecedor_nome = match.group(1).strip()
            ano = match.group(2).strip()
            
        # Filtra a base com pandas (assumindo colunas Nome Fornecedor e Data)
            pedidos_chat = pedidos_chat[
                pedidos_chat["Nome Fornecedor"].str.contains(fornecedor_nome, case=False, na=False) &
                pedidos_chat["Ano"].astype(str).str.contains(ano)
            ]
        
            if not pedidos_chat.empty:
                    pedidos_chat_str = pedidos_chat.to_markdown(index=False)
    
                # Envia uma única chamada para a OpenAI
                    system_prompt = f"""
                    #Role
                    Você é um pesquisador financeiro sênior, especializado em busca e análise de base de pedidos de compras para fornecedores.
                    
                    #Objetivo
                    
                    Sua tarefa é analisar a base de dados abaixo e responder de forma clara, concisa e objetiva.
                    O usuário pode perguntar tanto em termos de pedidos emitidos, faturamento ou valores pagos. Responda em todos os casos com os valores da base de dados para o ano questionado.
            
                    **Instruções:**
                    - Se a informação solicitada estiver na base de dados, forneça um único resumo preciso.
                    - Se a informação **não estiver disponível**, diga claramente "Os dados fornecidos não incluem essa informação".
                    - Não repita a mesma informação múltiplas vezes.
                    - Estruture sua resposta de forma clara.
                    
                    #Exemplos
                    
                    Caso o usuário pergunte quanto faturamos para o fornecedor AASJ SERVICOS INDUSTRIAIS EIRELI em 2025?:
                        Resposta: Em 2025 foram identificados 55 Pedidos de Compras para o fornecedor AASJ SERVICOS INDUSTRIAIS EIRELI, num montante total de R$ 12.680.242.
                        a quantidade de pedidos são os valores únicos da coluna Numero PO e o montante total é a soma dos valores da coluna Valor
                        o fornecedor questionado pelo usuário se encontra na coluna Nome Fornecedor
                    
                    Caso o usuário pergunte quanto faturamos para o fornecedor PETROHOUSE APOIO A EXPLORACAO DE PETROLEO LTDA em 2025?:
                        Resposta: Em 2025 foram identificados 08 Pedidos de Compras para o fornecedor PETROHOUSE APOIO A EXPLORACAO DE PETROLEO LTDA, num montante total de R$ 1.602.876.
                        a quantidade de pedidos são os valores únicos da coluna Numero PO e o montante total é a soma dos valores da coluna Valor
                        o fornecedor questionado pelo usuário se encontra na coluna Nome Fornecedor
                        
                    Caso o usuário pergunte quanto faturamos para o fornecedor CMM OFFSHORE BRASIL LTDA em 2025?:
                        Resposta: Em 2025 foram identificados 19 Pedidos de Compras para o fornecedor CMM OFFSHORE BRASIL LTDA, num montante total de R$ 35.282.933.
                        a quantidade de pedidos são os valores únicos da coluna Numero PO e o montante total é a soma dos valores da coluna Valor
                        o fornecedor questionado pelo usuário se encontra na coluna Nome Fornecedor
                        
                    Caso o usuário pergunte quanto faturamos para o fornecedor AASJ SERVICOS INDUSTRIAIS EIRELI em 2024?:
                        Resposta: Em 2024 foram identificados 330 Pedidos de Compras para o fornecedor AASJ SERVICOS INDUSTRIAIS EIRELI, num montante total de R$ 100.940.112.
                        a quantidade de pedidos são os valores únicos da coluna Numero PO e o montante total é a soma dos valores da coluna Valor
                        o fornecedor questionado pelo usuário se encontra na coluna Nome Fornecedor
                        
                    Caso o usuário pergunte quanto faturamos para o fornecedor PETROHOUSE APOIO A EXPLORACAO DE PETROLEO LTDA em 2024?:
                        Resposta: Em 2024 foram identificados 92 Pedidos de Compras para o fornecedor PETROHOUSE APOIO A EXPLORACAO DE PETROLEO LTDA, num montante total de R$ 42.144.166.
                        a quantidade de pedidos são os valores únicos da coluna Numero PO e o montante total é a soma dos valores da coluna ValorL
                        o fornecedor questionado pelo usuário se encontra na coluna Nome Fornecedor
                        
                    Caso o usuário pergunte quanto faturamos para o fornecedor CMM OFFSHORE BRASIL LTDA em 2024?:
                        Resposta: Em 2024 foram identificados 36 Pedidos de Compras para o fornecedor CMM OFFSHORE BRASIL LTDA, num montante total de R$ 161.236.617.
                        a quantidade de pedidos são os valores únicos da coluna Numero PO e o montante total é a soma dos valores da coluna Valor
                        o fornecedor questionado pelo usuário se encontra na coluna Nome Fornecedor
                        
                    **Responda com a base de dados abaixo:**
                    
                    {pedidos_chat_str}
                    """
        
                # Criar payload para a API OpenAI
                    payload = {
                        "model": "gpt-4o",
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": prompt_chat}
                        ],
                        "temperature": 0.2,
                        "max_tokens": 500  # Garante que a resposta não seja muito longa
                    }
            
                    # Configurar cabeçalhos da requisição
                    headers = {
                        "Authorization": f"Bearer {API_KEY}",
                        "Content-Type": "application/json"
                    }
            
                    # Enviar requisição à OpenAI com SSL DESATIVADO
                    response = requests.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers=headers,
                        json=payload,
                        verify=False  # ⚠️ DESATIVANDO SSL (ÚLTIMO RECURSO)
                    )
    
            # Verificar se a resposta foi bem-sucedida
                    if response.status_code == 200:
                        resposta_parte = response.json()["choices"][0]["message"]["content"]
                        resposta_final = resposta_parte
                    else:
                        resposta_final = f"Erro na API: {response.status_code} - {response.text}"
            else:
                        resposta_final = "⚠️ Nenhum dado encontrado para o fornecedor e ano mencionados."
    
        else:
            resposta_final = "❓ Não consegui identificar o fornecedor e ano na sua pergunta."
            
    # REVISOR: valida a resposta antes de exibir ao usuário
    
    headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

    revisor_prompt = """
    Você é um revisor de um perito financeiro voltado para clareza e padronização textual.
    
    Sua função é revisar a resposta de um perito financeiro garantindo que:
    - A linguagem esteja clara e direta.
    - A estrutura da resposta siga o padrão: quantidade de pedidos + fornecedor + ano + valor total.
    - A estrutura da resposta siga o padrão: numero do contrato + fornecedor + ano + valor total.
    - Evite qualquer repetição ou termos técnicos desnecessários.
    - Corrija erros gramaticais ou de formatação.
    - Mantenha os dados quantitativos e financeiros intactos.
    - As informações estão corretas de acordo com a base de dados de pedidos de compras.
    
    Fonte dos dados para verificar se o perito buscou as informações corretamente:
        
        {pedidos_chat_str}
    
    Responda apenas com a versão revisada.

    """
    
    payload_revisor = {
        "model": "gpt-4o", 
        "messages": [
            {"role": "system", "content": revisor_prompt},
            {"role": "user", "content": resposta_final}
        ],
        "temperature": 0.2,
        "max_tokens": 500
    }
    
    response_revisor = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json=payload_revisor,
        verify=False
    )
    
    if response_revisor.status_code == 200:
        resposta_final = response_revisor.json()["choices"][0]["message"]["content"]
    else:
        resposta_final = f"(Erro ao revisar resposta: {response_revisor.status_code})\n\n{resposta_final}"
        
    with st.chat_message("assistant"):
                st.write(f"{resposta_final}")

with aba4:
    
    # Título da Aplicação
    st.title('DADOS BRUTOS')

    # Converter a coluna de data corretamente
    df_bruto = base_pedidos.copy()
    df_bruto.columns = pd.Index([str(col).strip() for col in df_bruto.columns])
    df_bruto.columns = df_bruto.columns.str.replace('–', '-')  # Substitui EN DASH por hífen normal
    df_bruto.columns = df_bruto.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')  # Remove acentos e caracteres especiais
    df_bruto['Data do Pedido'] = pd.to_datetime(df_bruto['Data do Pedido'], errors='coerce')
    df_bruto["Data do Pedido"] = df_bruto["Data do Pedido"].apply(
        lambda x: x.replace(year=2022) if x.year == 2202 else x
    )

    # Renomear a coluna de valor
    df_bruto = df_bruto.rename(columns={
        'Nome_Tipo_PO':'Tipo de PO',
        'Nome_Empresa': 'Nome da Empresa',
        'Nome_Planta':'Nome da Planta',
        'Item_PO':'Item',
        'Numero_Material':'Numero Material',
        'Nome_Material':'Material',
        'Valor_PO':'Valor da PO',
        'Moeda_PO':'Moeda',
        'Cambio_PO':'Cambio',
        'Data_Entrega':'Data da Entrega',
        'Valor_Item_BRL': 'Valor Item - R$',
        'Numero_NF':'Nota Fiscal',
        'Data_NF':'Data NF'})

    # Remover Colunas

    df_bruto = df_bruto.drop(columns=[
        'Chave_Fornecedor',
        'Check Work Days',
        'Quantidade_POs',
        'Ano',
        'Media_Geral',
        'Preco_Medio',
        'Dif_Preco',
        'Check Preco',
        'Chave Compliance',
        'Resultado',
        'Check Compliance',
        'Dia de Aprovacao'],
        errors = 'ignore')

    # Tratar Dados

    df_bruto['Ordem_Interna'] = df_bruto['Ordem_Interna'].replace('','Não Informado')
    df_bruto['Cargo'] = df_bruto['Cargo'].replace('','Não Informado')
    df_bruto['Cargo'] = df_bruto['Cargo'].fillna('Não Informado')
    df_bruto['Area Autorizador'] = df_bruto['Area Autorizador'].replace('','Não Informado')
    df_bruto['Area Autorizador'] = df_bruto['Area Autorizador'].fillna('Não Informado')
    df_bruto['Nota Fiscal'] = df_bruto['Nota Fiscal'].replace('','Não Informado')

    check_fornecedor = st.sidebar.toggle('Verificar possível duplicidade?', value=False)

    if check_fornecedor == True:
        df_bruto = df_bruto[df_bruto['Check Fornecedor'] == 'Sim']
        
    # Expansor para seleção de colunas
    with st.expander('Colunas'):
        colunas = st.multiselect('Selecione', list(df_bruto.columns), list(df_bruto.columns))

    # Converter `st.date_input()` para datetime
    data_inicio = pd.to_datetime(data_pedido[0])
    data_fim = pd.to_datetime(data_pedido[1])

    # Aplicar os filtros corretamente (permitindo "Todos")
    dados_filtrados = df_bruto.copy()

    if fornecedor != "Todos":
        dados_filtrados = dados_filtrados[dados_filtrados['Nome Fornecedor'] == fornecedor]

    if area != "Todos":
        dados_filtrados = dados_filtrados[dados_filtrados['Area Autorizador'] == area]

    # Filtrar por Data
    dados_filtrados = dados_filtrados[
        (dados_filtrados['Data do Pedido'] >= data_inicio) &
        (dados_filtrados['Data do Pedido'] <= data_fim)
    ]

    # Filtrar por aprovador

    if aprovador != 'Todos':
        dados_filtrados = dados_filtrados[dados_filtrados['Aprovador Final'] == aprovador]

    # Selecionar apenas as colunas desejadas
    dados_filtrados = dados_filtrados[colunas]

    # Exibir a tabela filtrada
    st.dataframe(dados_filtrados)

    # Exibir a quantidade de linhas e colunas filtradas
    st.markdown(f'A tabela possui :blue[{dados_filtrados.shape[0]}] linhas e :blue[{dados_filtrados.shape[1]}] colunas.')

    # Input para nome do arquivo
    st.markdown('Escreva o nome do arquivo.')
    coluna1, coluna2 = st.columns(2)

    with coluna1:
        nome_arquivo = st.text_input('', label_visibility='collapsed', value='dados_pos') + '.xlsx'

    # Botão de download
    with coluna2:
        st.download_button(
        label="Fazer download em Excel",
        data=converte_excel(dados_filtrados),
        file_name=f"{nome_arquivo}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        on_click=mensagem_sucesso,
        key="download4"
        )
