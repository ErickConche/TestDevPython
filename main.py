from src import buscar_links
import pandas as pd
from datetime import datetime
import json
import sqlite3
from prefect import task, flow
from prefect import get_run_logger
from time import sleep

def links_salvos():
    with open('links.json') as f:
        links = json.load(f)
    return links

def year_month(date:datetime) -> int:
    return date.year *100 + date.month

@task
def converter_tipos(df, chave):
  '''Recebe um df de dados do BCB e retorna com as colunas data e outro valor nos formatos data e float''' 
  df['data'] = pd.to_datetime(df['data'])
  df[chave] = df[chave].astype(str).str.replace(',', '.').astype(float)
  return df

@flow
def persistir_dados(links, conexao):
  '''Baixa o dicionario de links e salva no banco com o nome da chave como tabela'''
  logger = get_run_logger()
  for link in links:
    df = buscar_links.analisar_link_csv(links, link)
    df = converter_tipos(df, link)
    logger.info(f'Persistindo dados da tabela {link}')
    df.to_sql(link, conexao, index=False, if_exists='replace', dtype={'data':'datetime', link:'float'})
    

@task
def agregar_dados_por_ano(tabela, conexao):
    logger = get_run_logger()
    df = pd.read_sql(f'select * from {tabela}', conexao)
    df['ano'] = pd.to_datetime(df['data']).dt.year
    logger.info(f'Agregando tabela {tabela} por ano')
    agg = df.groupby('ano', as_index=False)[tabela].sum()
    agg.to_sql(f'{tabela}_por_ano', conexao, index=False, if_exists='replace')
    return agg

@task
def agregar_dados_por_ano_mes(tabela, conexao):
    logger = get_run_logger()
    col = 'ano_mes'
    df = pd.read_sql(f'select * from {tabela}', conexao)
    df[col] = pd.to_datetime(df['data']).apply(year_month)
    logger.info(f'Agregando tabela {tabela} por ano')
    agg = df.groupby(col, as_index=False)[tabela].sum()
    agg.to_sql(f'{tabela}_por_ano_mes', conexao, index=False, if_exists='replace')
    return agg

@flow
def main():
    logger = get_run_logger()
    navegador = buscar_links.abrir_pagina_balanca_comercial()
    palavras_chave = ['saldo', 'receita', 'despesa']
    links = {}
    try:
        for p in palavras_chave:
            if navegador.current_url != buscar_links.URL:
                navegador.get(buscar_links.URL)
            links[p] = buscar_links.buscar_url_csv(navegador, p)

    except Exception as e:
        logger.error(str(e))
        logger.info('Falha no web scrapping. Lendo arquivo de links.')
        # aconteceu do site ta off e não conseguir terminar o fluxo
        # apenas um backup caso aconteça de novo
        links = links_salvos()

    conexao = sqlite3.connect('sqlite/balanca.db', check_same_thread=False)
    persistir_dados(links, conexao)


    for link in links:
        agregar_dados_por_ano(link, conexao)
        agregar_dados_por_ano_mes(link, conexao)



if __name__ == "__main__":
    main()
