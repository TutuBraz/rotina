import feedparser
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from concurrent.futures import ThreadPoolExecutor, as_completed
from langchain_community.document_loaders import WebBaseLoader
import threading
import time

# --- CONFIGURAÇÕES E CONSTANTES ---
DEFAULT_FEEDS = {
    'Xp Investimentos': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/5822277793724032524',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/13683415329780998272',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/10269129280916203083',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/9550017878123396804',
        'https://news.google.com/rss/search?q=xp%20investimentos%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    "Vinci": [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/5394093770400447553',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/7598054495566054039',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/13734905907790337986',
        'https://news.google.com/rss/search?q=Vinci%20Compass%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    'Tivio': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/15089636150786167689',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/14878059582149958709',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/10293027038187432395',
        'https://news.google.com/rss/search?q=tivio%20capita%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    'Tarpon': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/11130384113973110931',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/13474059744439098265',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/7060813333020958445',
        'https://news.google.com/rss/search?q=tarpon%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    'Bnp': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/15848728452539530082',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/14146215433246553046',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/14146215433246553144',
        'https://news.google.com/rss/search?q=bnp%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    'Oceana': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/4978498206918616829',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/15423088151033479411',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/10556121544099713022',
        'https://news.google.com/rss/search?q=oceana%20investimentos%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ]
}
MAX_WORKERS = 8
thread_local = threading.local()
NOME_ARQUIVO_HISTORICO = r'C:\Users\Arthur Braz\monitoramento_midia\noticias_para_historico.csv'
NOME_ARQUIVO_SAIDA_DIARIA = 'noticias_para_analise.csv'

# --- FUNÇÕES OTIMIZADAS ---

def get_selenium_driver():
    """Cria e retorna uma instância do driver Selenium para a thread atual, se ainda não existir."""
    driver = getattr(thread_local, 'driver', None)
    if driver is None:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument('--log-level=3')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        try:
            driver = webdriver.Chrome(options=chrome_options)
            setattr(thread_local, 'driver', driver)
        except WebDriverException as e:
            print(f"ERRO CRÍTICO: Não foi possível iniciar o WebDriver para esta thread. Erro: {e}")
            return None
    return driver

def obter_link_final_otimizado(url_google_news):
    """
    Função worker que obtém o link final de uma URL do Google News.
    Ela reutiliza o driver Selenium da sua própria thread.
    """
    driver = get_selenium_driver()
    if not driver: return None
    try:
        driver.set_page_load_timeout(10)
        driver.get(url_google_news)
        WebDriverWait(driver, timeout=7, poll_frequency=.2).until(
            lambda dr: "news.google.com" not in dr.current_url
        )
        return driver.current_url
    except TimeoutException:
        return driver.current_url
    except Exception as e:
        print(f"AVISO: Erro ao processar {url_google_news[:50]}... -> {type(e).__name__}. URL retornada: {driver.current_url if driver else 'N/A'}")
        return driver.current_url if driver else None

def extrair_conteudo_worker(chave, url):
    """
    Função worker para extrair metadados de uma URL final usando WebBaseLoader.
    """
    try:
        loader = WebBaseLoader(url)
        data = loader.load()
        if not data:
            print(f"AVISO: Nenhum conteúdo carregado para: {url}")
            return None
        metadata = data[0].metadata
        return {
            'gestora': chave,
            'titulo': metadata.get('title', 'N/A'),
            'subtitulo': metadata.get('description', 'N/A'),
            'url': metadata.get('source', url),
            'relevancia': None,
            'classificacao': None
        }
    except Exception as e:
        print(f"AVISO: Falha ao processar metadados de {url}: {e}")
        return None

# --- BLOCO PRINCIPAL DE EXECUÇÃO ---

def main():
    start_time = time.time()
    print("🚀 INICIANDO PROCESSO DE COLETA DE NOTÍCIAS 🚀")
    
    # ETAPA 0: Carregar histórico de URLs já processadas
    print(f"\n[ETAPA 0/4] Carregando histórico de notícias de '{NOME_ARQUIVO_HISTORICO}'...")
    try:
        df_historico = pd.read_csv(NOME_ARQUIVO_HISTORICO, sep=';')
        urls_historicas = set(df_historico['url'])
        print(f"✅ Etapa 0 concluída: {len(urls_historicas)} URLs encontradas no histórico.")
    except FileNotFoundError:
        urls_historicas = set()
        print(f"⚠️  Arquivo de histórico '{NOME_ARQUIVO_HISTORICO}' não encontrado. Nenhuma notícia será filtrada.")
    except Exception as e:
        urls_historicas = set()
        print(f"🚨 Erro ao ler o arquivo de histórico: {e}. O processo continuará sem histórico nesta execução.")

    # ETAPA 1: Coleta dos links dos feeds RSS
    print("\n[ETAPA 1/4] Coletando links dos feeds RSS...")
    tarefas_rss = []
    for chave, lista_feeds in DEFAULT_FEEDS.items():
        for url_feed in lista_feeds:
            feed = feedparser.parse(url_feed)
            for entry in feed.entries:
                tarefas_rss.append({'chave': chave, 'url_google': entry.link})
    print(f"✅ Etapa 1 concluída: {len(tarefas_rss)} links encontrados nos feeds.")

    # ETAPA 2: Resolução dos links do Google News e filtragem
    print(f"\n[ETAPA 2/4] Resolvendo {len(tarefas_rss)} links do Google News com {MAX_WORKERS} workers...")
    links_finais_brutos = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_tarefa = {executor.submit(obter_link_final_otimizado, t['url_google']): t for t in tarefas_rss}
        for i, future in enumerate(as_completed(future_to_tarefa)):
            tarefa_original = future_to_tarefa[future]
            print(f"  - Progresso: [{i + 1}/{len(tarefas_rss)}] Resolvido para '{tarefa_original['chave']}'...")
            try:
                url_final = future.result()
                if url_final and "news.google.com" not in url_final and url_final != tarefa_original['url_google']:
                    links_finais_brutos.append({'chave': tarefa_original['chave'], 'url_final': url_final})
            except Exception as exc:
                print(f"AVISO: Tarefa para {tarefa_original['chave']} gerou uma exceção: {exc}")
    
    urls_vistas = set()
    links_finais = []
    for link_info in links_finais_brutos:
        if link_info['url_final'] not in urls_vistas:
            links_finais.append(link_info)
            urls_vistas.add(link_info['url_final'])
    print(f"✅ Etapa 2 concluída: {len(links_finais)} links únicos resolvidos com sucesso.")
    
    links_a_processar = [
        tarefa for tarefa in links_finais if tarefa['url_final'] not in urls_historicas
    ]
    removidos = len(links_finais) - len(links_a_processar)
    print(f"  ->  Filtragem: {removidos} links removidos por já estarem no histórico.")

    if not links_a_processar:
        print("\n✅ Nenhuma notícia nova para processar. Encerrando.")
        end_time = time.time()
        print(f"\n🏁 PROCESSO CONCLUÍDO em {end_time - start_time:.2f} segundos. 🏁")
        return

    # ETAPA 3: Extração de metadados em paralelo
    print(f"\n[ETAPA 3/4] Extraindo conteúdo de {len(links_a_processar)} NOVOS links...")
    dados_para_df = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {
            executor.submit(extrair_conteudo_worker, tarefa['chave'], tarefa['url_final']): tarefa['url_final']
            for tarefa in links_a_processar
        }
        for i, future in enumerate(as_completed(future_to_url)):
            print(f"  - Progresso: [{i + 1}/{len(links_a_processar)}] Conteúdo extraído...")
            resultado = future.result()
            if resultado:
                dados_para_df.append(resultado)
    print(f"✅ Etapa 3 concluída: {len(dados_para_df)} conteúdos extraídos com sucesso.")

    # ETAPA 4: Montagem e salvamento do DataFrame do dia
    print(f"\n[ETAPA 4/4] Gerando e salvando o arquivo de análise de hoje...")
    if dados_para_df:
        noticias_para_analise = pd.DataFrame(dados_para_df)
        noticias_para_analise.to_csv(NOME_ARQUIVO_SAIDA_DIARIA, index=False, sep=';',encoding='utf-8-sig')
        print(f"✅ Etapa 4 concluída: Arquivo '{NOME_ARQUIVO_SAIDA_DIARIA}' salvo com {len(noticias_para_analise)} novas notícias.")
    else:
        print("⚠️ Nenhuma notícia nova foi processada com sucesso.")

    end_time = time.time()
    print(f"\n🏁 PROCESSO CONCLUÍDO em {end_time - start_time:.2f} segundos. 🏁")

if __name__ == "__main__":
    main()