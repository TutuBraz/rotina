import time
import threading
import os
from dotenv import load_dotenv
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError

import requests
import feedparser
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException

from langchain_community.document_loaders import WebBaseLoader

# --- CONFIGURAÇÕES DE DB E AMBIENTE (PADRÃO CI/CD) ---
load_dotenv()
DB_URL = os.getenv("DB_URL", "sqlite:///./data/noticias_pipeline.db")
TABLE_NAME = "noticias" # Tabela única para todas as notícias

# --- CONFIGURAÇÕES DE SCRAPER E CONSTANTES ---
DEFAULT_FEEDS = {
    'Xp Investimentos': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/5822277793724032524',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/13683415329780998272',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/10269129280916203083',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/9550017878123396804',
        'https://news.google.com/rss/search?q=xp%20investimentos%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    'Itaú Unibanco Asset Management Ltda': ['https://www.google.com.br/alerts/feeds/09404460482838700245/4932329740573989925'],
    'Azimut Brasil Wealth Management LTDA':['https://www.google.com.br/alerts/feeds/09404460482838700245/10530464050130697634'],
    'Franklin Templeton Investimentos':['https://www.google.com.br/alerts/feeds/09404460482838700245/18048358504902905705'],
    'BNP Paribas Asset Manegement Brasil LTDA': ['https://www.google.com.br/alerts/feeds/09404460482838700245/12308770235898626876'],
    'BTG Pactual Asset Management S.A. Distribuidora de Títulos e Valores Mobiliários': ['https://www.google.com.br/alerts/feeds/09404460482838700245/12181840149050642855'],
    'Sparta Administradora de Recursos Ltda':['https://www.google.com.br/alerts/feeds/09404460482838700245/16265448997652749518'],
    'Banco Bradesco S.A.':['https://www.google.com.br/alerts/feeds/09404460482838700245/1449991009216472773'],
    'CA Indosuez Wealth Brasil S.A. Distribuidora de Títulos e Valores Mobiliários': ['https://www.google.com.br/alerts/feeds/09404460482838700245/15945730803567192722'],
    'AZ Quest MZK Investimentos Macro e Crédito Ltda': ['https://www.google.com.br/alerts/feeds/09404460482838700245/16956946746322893208'],
    'Guepardo Investimentos Ltda':['https://www.google.com.br/alerts/feeds/09404460482838700245/9958653254634564973'],
    'Santander Brasil Gestão de Recursos Ltda':['https://www.google.com.br/alerts/feeds/09404460482838700245/7486324760529013833'],
    'Trígono Capital Ltda':['https://www.google.com.br/alerts/feeds/09404460482838700245/15814324762007185971'],
    '4UM Gestão de Recursos Ltda': ['https://www.google.com.br/alerts/feeds/09404460482838700245/17036973716228016199'],
    'Itaú Unibanco S.A.':['https://www.google.com.br/alerts/feeds/09404460482838700245/5347735920652075923'],
    'Safra Asset Management':['https://www.google.com.br/alerts/feeds/09404460482838700245/6440690998509546498'],
    'BB Gestão de Recursos':['https://www.google.com.br/alerts/feeds/09404460482838700245/7008359130834408114'],
    'BRAM - Bradesco Asset': ['https://www.google.com.br/alerts/feeds/09404460482838700245/5419144194917290392'],
    "Vinci": ['https://www.google.com.br/alerts/feeds/09404460482838700245/5394093770400447553', 'https://www.google.com.br/alerts/feeds/09404460482838700245/7598054495566054039', 'https://www.google.com.br/alerts/feeds/09404460482838700245/13734905907790337986', 'https://news.google.com/rss/search?q=Vinci%20Compass%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'],
    'Tivio': ['https://www.google.com.br/alerts/feeds/09404460482838700245/15089636150786167689', 'https://www.google.com.br/alerts/feeds/09404460482838700245/14878059582149958709', 'https://www.google.com.br/alerts/feeds/09404460482838700245/10293027038187432395', 'https://news.google.com/rss/search?q=tivio%20capita%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'],
    'Tarpon': ['https://www.google.com.br/alerts/feeds/09404460482838700245/11130384113973110931', 'https://www.google.com.br/alerts/feeds/09404460482838700245/13474059744439098265', 'https://www.google.com.br/alerts/feeds/09404460482838700245/7060813333020958445', 'https://news.google.com/rss/search?q=tarpon%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'],
    'Bnp': ['https://www.google.com.br/alerts/feeds/09404460482838700245/15848728452539530082', 'https://www.google.com.br/alerts/feeds/09404460482838700245/14146215433246553046', 'https://www.google.com.br/alerts/feeds/09404460482838700245/14146215433246553144', 'https://news.google.com/rss/search?q=bnp%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'],
    'Oceana': ['https://www.google.com.br/alerts/feeds/09404460482838700245/4978498206918616829', 'https://www.google.com.br/alerts/feeds/09404460482838700245/15423088151033479411', 'https://www.google.com.br/alerts/feeds/09404460482838700245/10556121544099713022', 'https://news.google.com/rss/search?q=oceana%20investimentos%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419']
}


MAX_WORKERS_FEEDS = int(os.getenv("MAX_WORKERS_FEEDS", 8))
MAX_WORKERS_SELENIUM = int(os.getenv("MAX_WORKERS_SELENIUM", 4))
MAX_WORKERS_EXTRACAO = int(os.getenv("MAX_WORKERS_EXTRACAO", 8))

thread_local = threading.local()
DRIVERS_CRIADOS = []
HEADERS = {"User-Agent": "Mozilla/5.0 (feed-fetcher/1.0)"}

# ---------------------- FUNÇÕES DE BANCO DE DADOS (NOVO) ----------------------
def get_db_engine():
    """Cria e verifica a engine do SQLAlchemy."""
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as connection:
            connection.execute(text(f"SELECT 1"))
        print(f"✅ Conexão com o DB estabelecida: {DB_URL.split('://')[0]}")
        return engine
    except SQLAlchemyError as e:
        print(f"🚨 ERRO CRÍTICO: Não foi possível conectar ao banco de dados em {DB_URL}. Erro: {e}")
        raise RuntimeError("Falha na conexão com o Banco de Dados.")

def get_urls_historicas_db(engine) -> set:
    """Consulta o DB para obter todas as URLs existentes (histórico)."""
    print(f"Buscando URLs existentes na tabela '{TABLE_NAME}'...")
    try:
        # Consulta para obter todas as URLs
        df = pd.read_sql(f"SELECT url FROM {TABLE_NAME}", engine)
        urls = set(df['url'].astype(str).tolist())
        return urls
    except Exception as e:
        # Se a tabela não existe (primeira execução), retorna um set vazio
        if "no such table" in str(e).lower() or "does not exist" in str(e).lower():
            print(f"⚠️ A tabela '{TABLE_NAME}' não existe. Será criada na primeira escrita.")
            return set()
        print(f"🚨 Erro ao buscar histórico no DB: {e}")
        # Em caso de falha não crítica, ainda permite que o scraper continue, mas avisa.
        return set()

def save_to_db(df: pd.DataFrame, engine):
    """Salva o DataFrame no banco de dados com tratamento de duplicidade."""
    print(f"Salvando {len(df)} novos registros na tabela '{TABLE_NAME}'...")
    try:
        df.to_sql(
            TABLE_NAME, 
            engine, 
            if_exists='append', 
            index=False,
            # Se fosse PostgreSQL, poderia usar: conflict='ignore', se a coluna 'url' for UNIQUE
        )
        print(f"✅ {len(df)} registros salvos com sucesso no DB.")
    except IntegrityError as e:
        # Geralmente, indica que uma URL duplicada tentou ser inserida (se a coluna 'url' for UNIQUE)
        print(f"⚠️ Aviso: Falha de Integridade (Duplicidade). Alguns registros foram ignorados pelo DB.")
    except Exception as e:
        print(f"🚨 ERRO CRÍTICO ao salvar dados no DB: {e}")
        raise

# ---------------------- UTILITÁRIOS E SCRAPER (INALTERADO) ----------------------

# ... (Funções get_with_retries, baixar_feed, coletar_links_feeds, _eh_intermediario_google, precisa_selenium, get_selenium_driver, obter_link_final_otimizado, _texto_suspeito, _invalida_por_conteudo, extrair_conteudo_worker) ...
# O código das funções utilitárias do Scraper e Selenium permanecem o mesmo, mas a função ler_somente_urls foi removida.

def get_with_retries(url: str, tries: int = 3, backoff_base: float = 2.0):
    for i in range(tries):
        try:
            return requests.get(url, headers=HEADERS, timeout=(5, 15), allow_redirects=True)
        except Exception:
            if i == tries - 1:
                raise
            sleep_for = (backoff_base ** i) + (0.1 * i)
            time.sleep(sleep_for)

def baixar_feed(url: str):
    t0 = time.time()
    resp = get_with_retries(url, tries=3)
    resp.raise_for_status()
    parsed = feedparser.parse(resp.content)
    dt = time.time() - t0
    print(f"    · Feed carregado em {dt:.2f}s: {url[:100]}")
    return parsed

def coletar_links_feeds(default_feeds: dict, max_workers: int = 8):
    tarefas = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futuros = {
            ex.submit(baixar_feed, feed): (chave, feed)
            for chave, feeds in default_feeds.items()
            for feed in feeds
        }
        for fut in as_completed(futuros):
            chave, feed = futuros[fut]
            try:
                parsed = fut.result()
                for entry in getattr(parsed, 'entries', []):
                    link = getattr(entry, 'link', None)
                    if link:
                        tarefas.append({'chave': chave, 'url_google': link})
            except Exception as e:
                print(f"AVISO: Falha ao baixar feed '{feed}' ({chave}): {e}")
    return tarefas

def _eh_intermediario_google(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
        return host.endswith("google.com")
    except Exception:
        return False

def precisa_selenium(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith("google.com")

def get_selenium_driver():
    driver = getattr(thread_local, 'driver', None)
    if driver is None:
        chrome_options = Options()
        chrome_options.page_load_strategy = "eager"
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-features=HeavyAdIntervention")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        try:
            driver = webdriver.Chrome(options=chrome_options)
            setattr(thread_local, 'driver', driver)
            DRIVERS_CRIADOS.append(driver)
        except WebDriverException as e:
            print(f"ERRO CRÍTICO: Não foi possível iniciar o WebDriver para esta thread. Erro: {e}")
            return None
    return driver

def obter_link_final_otimizado(url_google_news: str):
    driver = get_selenium_driver()
    if not driver:
        return None
    try:
        driver.set_page_load_timeout(8)
        start = url_google_news
        driver.get(start)

        t0 = time.time()
        while time.time() - t0 < 6:
            cur = driver.current_url or ""
            if cur != start and not _eh_intermediario_google(cur):
                return cur
            time.sleep(0.2)
        return None
    except TimeoutException:
        cur = driver.current_url
        return cur if (cur and cur != url_google_news and not _eh_intermediario_google(cur)) else None
    except Exception as e:
        print(f"AVISO Selenium: {type(e).__name__} em {url_google_news[:60]}...")
        return None

# -------- Regras de descarte --------
def _texto_suspeito(txt: str) -> bool:
    if not txt:
        return True
    t = txt.strip().lower()
    gatilhos = [
        "just a moment", "access denied", "403 forbidden", "request blocked",
        "checking your browser", "captcha", "forbidden", "blocked",
        "not authorized", "temporarily unavailable"
    ]
    return any(g in t for g in gatilhos)

MIN_TITULO_CHARS = 8
MIN_TOTAL_CHARS = 12
BLACKLIST_TITULOS = {"home", "login", "index of", "redirecting", "oops", "error"}

def _invalida_por_conteudo(titulo: str, subtitulo: str) -> bool:
    titulo = (titulo or "").strip()
    subtitulo = (subtitulo or "").strip()
    if _texto_suspeito(titulo) or _texto_suspeito(subtitulo):
        return True
    if not titulo and not subtitulo:
        return True
    if len(titulo) < MIN_TITULO_CHARS and len((titulo + " " + subtitulo).strip()) < MIN_TOTAL_CHARS:
        return True
    if titulo.lower() in BLACKLIST_TITULOS:
        return True
    return False

def extrair_conteudo_worker(chave, url):
    try:
        try:
            loader = WebBaseLoader(url, requests_kwargs={"timeout": (5, 15)})
        except TypeError:
            loader = WebBaseLoader(url)

        data = loader.load()
        if not data:
            print(f"    · DESCARTADO: nenhum conteúdo retornado — {url[:90]}")
            return None

        metadata = data[0].metadata or {}
        titulo = (metadata.get('title', '') or '').strip()
        subtitulo = (metadata.get('description', '') or '').strip()

        if _invalida_por_conteudo(titulo, subtitulo):
            print(f"    · DESCARTADO: conteúdo inválido/bloqueado — {url[:90]}")
            return None

        # Estrutura do DB: Note que os campos vazios (alvo, classificacao, etc) 
        # serão preenchidos pelas etapas subsequentes (E2, E3, etc)
        return {
            'gestora': chave,
            'titulo': titulo,
            'subtitulo': subtitulo,
            'url': metadata.get('source', url),
            'alvo': None,
            'classificacao': None,
            'interesse': None,
            'resposta_modelo': None,
            'texto': None,
            'descricao': None,
            'justificativa_alvo': None,
            'status_e2': 'PENDENTE', # NOVO: Ajuda na orquestração da próxima etapa (E2)
            'timestamp_e1': pd.Timestamp.now() # NOVO: Para registro do tempo de coleta
        }
    except Exception as e:
        print(f"    · DESCARTADO: erro na extração ({type(e).__name__}) — {url[:90]}")
        return None

# ---------------------- MAIN - FLUXO ORQUESTRADO ----------------------

def main():
    start_time = time.time()
    print("🚀 E1 - INICIANDO PROCESSO DE COLETA E INGESTÃO NO BANCO DE DADOS 🚀")
    
    # 0. Conectar ao DB e carregar histórico
    DB_ENGINE = None
    try:
        DB_ENGINE = get_db_engine()
        
        # ETAPA 0: Carregar URLs existentes do DB (Substitui CSV)
        urls_historicas = get_urls_historicas_db(DB_ENGINE)
        print(f"✅ Etapa 0 concluída: {len(urls_historicas)} URLs encontradas no histórico do DB.")

        # ETAPA 1: Coleta dos links dos feeds RSS
        print("\n[ETAPA 1/4] Coletando links dos feeds RSS...")
        tarefas_rss = coletar_links_feeds(DEFAULT_FEEDS, max_workers=MAX_WORKERS_FEEDS)
        print(f"✅ Etapa 1 concluída: {len(tarefas_rss)} links encontrados nos feeds.")

        if not tarefas_rss:
            print("\n⚠️ Nenhum link obtido dos feeds. Encerrando.")
            return

        # ETAPA 2: Resolver links e filtrar duplicatas/histórico
        print(f"\n[ETAPA 2/4] Resolvendo {len(tarefas_rss)} links do Google News com {MAX_WORKERS_SELENIUM} workers...")
        links_finais_brutos = []

        # 2.1 e 2.2 (Lógica de resolução de links mantida)
        for t in tarefas_rss:
            if not precisa_selenium(t['url_google']):
                links_finais_brutos.append({'chave': t['chave'], 'url_final': t['url_google']})

        tarefas_selenium = [t for t in tarefas_rss if precisa_selenium(t['url_google'])]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_SELENIUM) as executor:
            future_to_tarefa = {executor.submit(obter_link_final_otimizado, t['url_google']): t for t in tarefas_selenium}
            for i, future in enumerate(as_completed(future_to_tarefa)):
                t = future_to_tarefa[future]
                print(f"  - Progresso: [{i + 1}/{len(tarefas_selenium)}] Resolvido para '{t['chave']}'...")
                try:
                    url_final = future.result(timeout=20)
                    if url_final:
                        links_finais_brutos.append({'chave': t['chave'], 'url_final': url_final})
                except Exception:
                    pass # Erros já são logados na função obter_link_final_otimizado

        # deduplicação e filtro por DB
        urls_vistas = set()
        links_finais = []
        for link_info in links_finais_brutos:
            # 1. Deduplicação interna
            if link_info['url_final'] not in urls_vistas:
                urls_vistas.add(link_info['url_final'])
                
                # 2. Filtro por histórico do DB
                if link_info['url_final'] not in urls_historicas:
                    links_finais.append(link_info)

        removidos = len(links_finais_brutos) - len(links_finais)
        print(f"✅ Etapa 2 concluída. Links únicos após filtros: {len(links_finais)}")
        print(f"  ->  Total de links removidos (Duplicados/Histórico): {removidos}")


        if not links_finais:
            print("\n✅ Nenhuma notícia nova para processar. Encerrando.")
            return

        # ETAPA 3: Extração de metadados em paralelo
        print(f"\n[ETAPA 3/4] Extraindo metadados de {len(links_finais)} NOVOS links...")
        dados_para_df = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXTRACAO) as executor:
            future_to_url = {
                executor.submit(extrair_conteudo_worker, tarefa['chave'], tarefa['url_final']): tarefa['url_final']
                for tarefa in links_finais
            }
            for i, future in enumerate(as_completed(future_to_url)):
                print(f"  - Progresso: [{i + 1}/{len(links_finais)}] Conteúdo extraído...")
                try:
                    resultado = future.result()
                    if resultado:
                        dados_para_df.append(resultado)
                except Exception as e:
                    print(f"AVISO: Future falhou ({type(e).__name__})")
                    continue

        print(f"✅ Etapa 3 concluída: {len(dados_para_df)} conteúdos válidos extraídos.")
        descartados = len(links_finais) - len(dados_para_df)
        print(f"  -> Descuidos/Inválidos descartados nesta fase: {descartados}")

        # ETAPA 4: SALVAMENTO NO BANCO DE DADOS (Substitui CSV)
        if dados_para_df:
            noticias_para_analise = pd.DataFrame(dados_para_df)
            save_to_db(noticias_para_analise, DB_ENGINE)
            print(f"✅ Etapa 4 concluída: {len(noticias_para_analise)} novas notícias inseridas no DB para análise subsequente (E2).")
        else:
            print("⚠️ Nenhuma notícia válida foi processada. Nada foi inserido no DB.")

    except RuntimeError as e:
        # Captura erros críticos como falha na conexão com o DB
        print(f"\n🚨 ERRO CRÍTICO NO FLUXO: {e}")
    except Exception as e:
        print(f"\n🚨 ERRO GERAL NO FLUXO: {e}")
    
    finally:
        for d in set(DRIVERS_CRIADOS):
            try:
                d.quit()
            except Exception:
                pass
        end_time = time.time()
        print(f"\n🏁 PROCESSO E1 CONCLUÍDO em {end_time - start_time:.2f} segundos. 🏁")

if __name__ == "__main__":
    main()