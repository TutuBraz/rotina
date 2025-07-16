# ==================== BIBLIOTECAS ====================
import os
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.service import Service 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime
import requests
from dotenv import load_dotenv

# Import para o sistema de logging
import logging

# ==================== CONFIGURAÇÕES ====================
load_dotenv() 

# Função para configurar o logging
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

PALAVRAS_CHAVE = ['Tivio', 'xp', 'vinci', 'tarpon', 'bnp', 'oceana']
URL_BASE_CVM = "https://www.gov.br/cvm/pt-br/search?origem=form&SearchableText={}"
CHAT_WEBHOOK_URL_MUNIN = os.getenv("CHAT_WEBHOOK_URL")

# ==================== FUNÇÕES ====================

def localiza_news(driver, palavra_chave):
    """Busca a notícia mais recente para uma palavra-chave no site da CVM."""
    url = URL_BASE_CVM.format(palavra_chave)
    driver.get(url)

    wait = WebDriverWait(driver, 10)

    try:
        botao_rejeitar = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "button.reject-all")
        ))
        botao_rejeitar.click()
        logging.info(f"[{palavra_chave}] Botão de cookies rejeitado com sucesso.")
    except TimeoutException:
        # Isso não é um erro, apenas um estado da página.
        logging.info(f"[{palavra_chave}] Janela de cookies não encontrada ou não precisou de clique.")
        pass

    try:
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
        logging.info(f"[{palavra_chave}] Nenhum resultado encontrado na página.")
        return None
    except Exception as e:
        # Usar exc_info=True para logar o traceback completo do erro
        logging.error(f"[{palavra_chave}] Erro inesperado ao extrair dados: {e}", exc_info=True)
        return None

def envia_alerta_munin(gestora, titulo, link, data):
    """Envia uma mensagem de alerta para o Google Chat."""
    if not CHAT_WEBHOOK_URL_MUNIN:
        logging.error("A variável de ambiente CHAT_WEBHOOK_URL não está configurada. Alerta não enviado.")
        return

    mensagem = {
        "text": f"🚨 *Alerta CVM* 🚨\n\nA gestora *{gestora}* foi noticiada no site da CVM:\n\n*Data:* {data}\n*Título:* {titulo}\n*Link:* {link}"
    }
    
    try:
        response = requests.post(CHAT_WEBHOOK_URL_MUNIN, json=mensagem, timeout=10)
        response.raise_for_status() # Lança um erro para respostas HTTP 4xx/5xx
        logging.info(f"[{gestora}] Alerta enviado com sucesso!")
    except requests.exceptions.RequestException as e:
        logging.error(f"[{gestora}] Falha ao enviar alerta para o Google Chat: {e}")

def main():
    """Função principal que orquestra a execução do robô."""
    logging.info("="*20 + " INICIANDO ROBÔ DE MONITORAMENTO DA CVM " + "="*20)
    
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    
    driver = webdriver.Chrome(service=service, options=options)
    
    hoje = datetime.now().date()
    noticias_encontradas = 0

    try:
        for gestora in PALAVRAS_CHAVE:
            logging.info(f"Buscando por: '{gestora}'...")
            noticia = localiza_news(driver, gestora)

            if noticia and noticia["DataObj"] and noticia["DataObj"].date() == hoje:
                logging.info(f"[{gestora}] Notícia encontrada para a data de hoje! Enviando alerta.")
                envia_alerta_munin(noticia["Gestora"], noticia["Título"], noticia["Link"], noticia["Data"])
                noticias_encontradas += 1
            elif noticia:
                logging.info(f"[{gestora}] Notícia encontrada, mas não é de hoje (Data: {noticia['Data']}).")

            sleep(1) # Pequena pausa para não sobrecarregar o site

    finally:
        logging.info(f"Busca finalizada. Total de {noticias_encontradas} notificações enviadas hoje.")
        logging.info("="*25 + " ROBÔ FINALIZADO " + "="*25)
        driver.quit()

if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except Exception as e:
        logging.critical("Ocorreu um erro fatal e não tratado na execução do robô.", exc_info=True)
        # Re-lança a exceção para que o Jenkins marque o build como falho
        raise