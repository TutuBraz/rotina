#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ==================== BIBLIOTECAS ====================
import os
import sys # Adicionado para sys.exit (boa prática em funções main)
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.service import Service 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from datetime import datetime, date
import requests
import logging
from dotenv import load_dotenv

# --- Banco de dados ---
import sqlite3
from typing import Optional

# ==================== FUNÇÃO DE LOGGING ====================
def setup_logging():
    """Configura o logging para exibir mensagens formatadas no console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Reduz o log excessivo de bibliotecas de terceiros
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium.webdriver.remote").setLevel(logging.WARNING)

# ==================== CONFIGURAÇÕES E PATHS (CI/CD) ====================
load_dotenv() 

PALAVRAS_CHAVE = ['Tivio', 'xp investimentos', 'vinci', 'tarpon', 'bnp', 'oceana']
URL_BASE_CVM = "https://www.gov.br/cvm/pt-br/search?origem=form&SearchableText={}"
CHAT_WEBHOOK_URL_MUNIN = os.getenv("CHAT_WEBHOOK_URL_MUNIN")

# PADRÃO CI/CD: Define o caminho do DB dentro da pasta de dados persistente
DB_FILENAME = "cvm_sent.db"
# NOTE: O Actions injeta as variáveis de ambiente, senão usa "./data"
DB_DIR = os.environ.get("DATA_DIR", "./data") 
DB_PATH = os.path.join(DB_DIR, DB_FILENAME)

# ==================== DB FUNÇÕES ====================

def db_init(db_path: str = DB_PATH) -> Optional[sqlite3.Connection]:
    """
    Cria (se não existir) e retorna a conexão com o banco SQLite.
    """
    try:
        # Garante que a pasta 'data' exista antes de criar o arquivo DB
        os.makedirs(os.path.dirname(db_path), exist_ok=True) 
        
        con = sqlite3.connect(db_path)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_notifications (
                sent_date TEXT NOT NULL, 
                gestora   TEXT NOT NULL,
                link      TEXT NOT NULL,
                title     TEXT,
                sent_at   TEXT NOT NULL,
                PRIMARY KEY (sent_date, gestora, link)
            )
            """
        )
        con.commit()
        return con
    except Exception as e:
        logging.critical(f"Falha crítica ao inicializar o banco de dados em '{db_path}': {e}", exc_info=True)
        return None # Retorna None se a inicialização falhar

def iso(d: date) -> str:
    """Retorna data no formato YYYY-MM-DD."""
    return d.isoformat()

def already_sent_today(con: sqlite3.Connection, sent_date: date, gestora: str, link: str) -> bool:
    cur = con.execute(
        "SELECT 1 FROM sent_notifications WHERE sent_date = ? AND gestora = ? AND link = ?",
        (iso(sent_date), gestora, link)
    )
    return cur.fetchone() is not None

def mark_sent(con: sqlite3.Connection, sent_date: date, gestora: str, link: str, title: str):
    con.execute(
        """
        INSERT OR IGNORE INTO sent_notifications(sent_date, gestora, link, title, sent_at)
        VALUES(?, ?, ?, ?, ?)
        """,
        (iso(sent_date), gestora, link, title, datetime.utcnow().isoformat())
    )
    con.commit()

# ==================== FUNÇÕES DE EXTRAÇÃO E ALERTA ====================

def localiza_news(driver, palavra_chave):
    """Busca a notícia mais recente para uma palavra-chave no site da CVM."""
    url = URL_BASE_CVM.format(palavra_chave)
    driver.get(url)

    wait = WebDriverWait(driver, 10)

    try:
        # Tenta rejeitar cookies (pode ser o que está mudando o layout na VM)
        botao_rejeitar = wait.until(EC.element_to_be_clickable(
             (By.CSS_SELECTOR, "button.reject-all")
           ))
        botao_rejeitar.click()
        logging.info(f"[{palavra_chave}] Botão de cookies rejeitado com sucesso.")
    except TimeoutException:
        logging.info(f"[{palavra_chave}] Janela de cookies não encontrada ou não precisou de clique.")
        pass # Segue em frente

    try:
        # Tenta encontrar o primeiro resultado na lista de notícias
        primeiro_resultado = wait.until(EC.presence_of_element_located(
             (By.CSS_SELECTOR, "ul.searchResults.noticias li:first-child")
           ))

        titulo_el = primeiro_resultado.find_element(By.CSS_SELECTOR, "span.titulo a")
        titulo = titulo_el.text.strip()
        link = titulo_el.get_attribute("href").strip()

        data_el = primeiro_resultado.find_element(By.CSS_SELECTOR, "span.data")
        data_str = data_el.text.strip().replace("-", "").strip()

        try:
            data_obj = datetime.strptime(data_str, '%d/%m/%Y')
        except ValueError:
            logging.error(f"[{palavra_chave}] Erro ao converter a data: '{data_str}'")
            data_obj = None

        return {
            "Gestora": palavra_chave,
            "Título": titulo,
            "Link": link,
            "Data": data_obj.strftime('%d/%m/%Y') if data_obj else "",
            "DataObj": data_obj
        }

    except TimeoutException:
        # Este é o cenário que está ocorrendo no Actions: elemento não encontrado
        logging.info(f"[{palavra_chave}] Nenhum resultado encontrado na página (Timeout).")
        return None
    except Exception as e:
        logging.error(f"[{palavra_chave}] Erro inesperado ao extrair dados: {e}", exc_info=True)
        return None

def envia_alerta_munin(gestora, titulo, link, data):
    """Envia uma mensagem de alerta para o Google Chat."""
    if not CHAT_WEBHOOK_URL_MUNIN:
        logging.error("A variável de ambiente CHAT_WEBHOOK_URL_MUNIN não está configurada. Alerta não enviado.")
        return

    mensagem = {
        "text": f"🚨 *Alerta CVM* 🚨\n\nA gestora *{gestora}* foi noticiada no site da CVM:\n\n*Data:* {data}\n*Título:* {titulo}\n*Link:* {link}"
    }
    
    try:
        response = requests.post(CHAT_WEBHOOK_URL_MUNIN, json=mensagem, timeout=10)
        response.raise_for_status()
        logging.info(f"[{gestora}] Alerta enviado com sucesso!")
    except requests.exceptions.RequestException as e:
        logging.error(f"[{gestora}] Falha ao enviar alerta para o Google Chat: {e}")

def main():
    """Função principal que orquestra a execução do robô."""
    logging.info("="*20 + " INICIANDO ROBÔ DE MONITORAMENTO DA CVM " + "="*20)
    
    # --- DB init ---
    con = db_init() 
    if not con:
        sys.exit(1) # Encerra se a inicialização do DB falhar

    service = Service()
    options = webdriver.ChromeOptions()
    
    # --- CONFIGURAÇÕES MELHORADAS PARA AMBIENTE ACTIONS (CI/CD) ---
    options.add_argument("--headless=new") # Modo headless moderno, mais robusto
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080") # Garante que a página seja renderizada em um tamanho padrão

    # 🚨 User-Agent Falso: Imita um navegador real para evitar bloqueios 🚨
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    driver = None
    try:
        driver = webdriver.Chrome(service=service, options=options)
    except WebDriverException as e:
        logging.critical(f"ERRO CRÍTICO: Não foi possível iniciar o WebDriver. Erro: {e}")
        # Se o driver não iniciar, encerra com erro
        if con: con.close()
        sys.exit(1)

    hoje = datetime.now().date()
    noticias_encontradas = 0
    notificacoes_enviadas = 0

    try:
        for gestora in PALAVRAS_CHAVE:
            logging.info(f"Buscando por: '{gestora}'...")
            noticia = localiza_news(driver, gestora)

            if noticia and noticia["DataObj"] and noticia["DataObj"].date() == hoje:
                
                if already_sent_today(con, hoje, gestora, noticia["Link"]):
                    logging.info(f"[{gestora}] Notícia de hoje já notificada (evitando duplicata).")
                else:
                    logging.info(f"[{gestora}] Notícia encontrada para a data de hoje! Enviando alerta.")
                    envia_alerta_munin(noticia["Gestora"], noticia["Título"], noticia["Link"], noticia["Data"])
                    mark_sent(con, hoje, gestora, noticia["Link"], noticia["Título"])
                    notificacoes_enviadas += 1
                noticias_encontradas += 1

            elif noticia:
                logging.info(f"[{gestora}] Notícia encontrada, mas não é de hoje (Data: {noticia['Data']}).")

            sleep(1) 

    finally:
        if driver:
            driver.quit()
        if con:
            con.close()
        
        logging.info(f"Busca finalizada. {noticias_encontradas} notícia(s) de hoje encontrada(s). {notificacoes_enviadas} notificação(ões) enviada(s) (sem duplicar).")
        logging.info("="*25 + " ROBÔ FINALIZADO " + "="*25)

if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except Exception as e:
        logging.critical("Ocorreu um erro fatal e não tratado na execução do robô.", exc_info=True)
        # O programa falha, o que é o comportamento correto para um fluxo CI/CD não tratado
        sys.exit(1) # Garante que o Actions reporte a falha