from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
from prefect import task, flow
from prefect import get_run_logger
URL = 'https://dadosabertos.bcb.gov.br/pages/temas#grp1762'

@task
def abrir_pagina_balanca_comercial():
    logger = get_run_logger()
    navegador = webdriver.Edge()
    URL = 'https://dadosabertos.bcb.gov.br/pages/temas#grp1762'
    logger.info(f'Navegando até {URL}')
    navegador.get(URL)

    return navegador

@task
def buscar_link_balanca_comercial(navegador, palavra_chave):
    _str = 'Balança comercial e Serviços - '
    elemento_saldo = navegador.find_element(By.XPATH, f'//*[text()[contains(.,"{_str + palavra_chave}")]]')
    link = elemento_saldo.get_attribute('href')
    return link

@task
def retornar_pagina_para_csv(navegador):
    _str = 'csv_serie-sgs'
    elemento_csv = navegador.find_element(By.XPATH, f'//*[text()[contains(.,"{_str}")]]')
    link = elemento_csv.get_attribute('href')
    return link 

@task    
def buscar_link_para_csv(navegador):
    _str = 'URL:'
    elemento_url = navegador.find_element(By.XPATH, f'//*[text()[contains(.,"{_str}")]]')
    link = elemento_url.find_element(By.TAG_NAME, f'a')
    link = link.get_attribute('href')
    return link

@flow
def buscar_url_csv(navegador, palavra_chave):
    logger = get_run_logger()
    novo_link = buscar_link_balanca_comercial(navegador, palavra_chave)
    navegador.get(novo_link)
    pagina_csv = retornar_pagina_para_csv(navegador)
    navegador.get(pagina_csv)
    url = buscar_link_para_csv(navegador)
    logger.info(f'Novo link para palavra chave {palavra_chave}: {url}')
    return url 

@task
def analisar_link_csv(links, chave) -> pd.DataFrame:
    logger = get_run_logger()
    saldo = pd.read_csv(links[chave], sep=';').rename(columns={'valor':chave})
    logger.info(f'Tabela {chave} baixada do link {links[chave]}')
    return saldo
