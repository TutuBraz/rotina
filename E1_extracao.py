import time
import threading
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from pathlib import Path

import requests
import feedparser
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException

from langchain_community.document_loaders import WebBaseLoader

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

# Paralelismo separado por etapa
MAX_WORKERS_FEEDS = 8       # etapa 1 (feeds)
MAX_WORKERS_SELENIUM = 4    # etapa 2 (selenium)
MAX_WORKERS_EXTRACAO = 8    # etapa 3 (webbase loader)

thread_local = threading.local()
NOME_ARQUIVO_HISTORICO = r'C:\Users\Arthur Braz\monitoramento_midia\noticias_para_historico.csv'
NOME_ARQUIVO_SAIDA_DIARIA = 'noticias_para_analise.csv'

# rastreia drivers para fechar no final
DRIVERS_CRIADOS = []

# headers para baixar feeds via requests
HEADERS = {"User-Agent": "Mozilla/5.0 (feed-fetcher/1.0)"}

# ---------------------- utilitários ----------------------
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
    print(f"    · Feed carregado em {dt:.2f}s: {url[:100]}")
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
MIN_TOTAL_CHARS  = 12
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
            print(f"    · DESCARTADO: nenhum conteúdo retornado — {url[:90]}")
            return None

        metadata = data[0].metadata or {}
        titulo = (metadata.get('title', '') or '').strip()
        subtitulo = (metadata.get('description', '') or '').strip()

        if _invalida_por_conteudo(titulo, subtitulo):
            print(f"    · DESCARTADO: conteúdo inválido/bloqueado — {url[:90]}")
            return None

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
            'justificativa_alvo': None
        }
    except Exception as e:
        print(f"    · DESCARTADO: erro na extração ({type(e).__name__}) — {url[:90]}")
        return None

# ---------------------- ETAPA 0 (EXTRA): LER SOMENTE A COLUNA URL ----------------------
def ler_somente_urls(caminho: str, sep: str = ';') -> set:
    """
    Lê apenas a coluna 'url' do histórico, de forma resiliente, sem usar o parser do pandas.
    Evita erros do tipo 'Expected N fields in line ... saw ...'.

    - Busca o índice da coluna 'url' no cabeçalho (case-insensitive).
    - Varre o arquivo linha a linha, usa split pelo separador e pega apenas o campo 'url'.
    - Linhas com menos colunas são ignoradas; com mais colunas não quebram a leitura.
    """
    p = Path(caminho)
    if not p.exists():
        print(f"⚠️  Arquivo de histórico '{caminho}' não encontrado.")
        return set()

    try:
        with p.open('r', encoding='utf-8-sig', errors='replace') as f:
            header = f.readline()
            if not header:
                print("⚠️  Histórico vazio.")
                return set()

            cols = [c.strip().lower() for c in header.rstrip('\n\r').split(sep)]
            if 'url' not in cols:
                print("⚠️  Cabeçalho do histórico não possui coluna 'url'.")
                return set()
            idx_url = cols.index('url')

            urls = set()
            for line in f:
                # remove quebras de linha e separa; linhas malformadas não derrubam o processo
                parts = line.rstrip('\n\r').split(sep)
                if len(parts) > idx_url:
                    u = parts[idx_url].strip()
                    if u:
                        urls.add(u)
            return urls
    except Exception as e:
        print(f"🚨 Erro ao ler o arquivo de histórico de forma resiliente: {e}")
        return set()

# ---------------------- PIPELINE PRINCIPAL ----------------------
def main():
    start_time = time.time()
    print("🚀 INICIANDO PROCESSO DE COLETA DE NOTÍCIAS 🚀")
    try:
        # ETAPA 0: Carregar histórico (apenas coluna URL, robusto)
        print(f"\n[ETAPA 0/4] Carregando histórico de notícias de '{NOME_ARQUIVO_HISTORICO}'...")
        urls_historicas = ler_somente_urls(NOME_ARQUIVO_HISTORICO, sep=';')
        print(f"✅ Etapa 0 concluída: {len(urls_historicas)} URLs encontradas no histórico (modo resiliente).")

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

        # 2.1: atalhos sem Selenium
        for t in tarefas_rss:
            if not precisa_selenium(t['url_google']):
                links_finais_brutos.append({'chave': t['chave'], 'url_final': t['url_google']})

        # 2.2: somente o que precisa de Selenium
        tarefas_selenium = [t for t in tarefas_rss if precisa_selenium(t['url_google'])]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_SELENIUM) as executor:
            future_to_tarefa = {executor.submit(obter_link_final_otimizado, t['url_google']): t for t in tarefas_selenium}
            for i, future in enumerate(as_completed(future_to_tarefa)):
                t = future_to_tarefa[future]
                print(f"  - Progresso: [{i + 1}/{len(tarefas_selenium)}] Resolvido para '{t['chave']}'...")
                try:
                    url_final = future.result(timeout=20)
                    if url_final:
                        links_finais_brutos.append({'chave': t['chave'], 'url_final': url_final})
                except FuturesTimeoutError:
                    print("AVISO: tarefa Selenium estourou timeout externo (20s).")
                except Exception as exc:
                    print(f"AVISO: exceção resolvendo link ({t['chave']}): {exc}")

        # dedup por URL final (sem normalização aqui para manter fiel ao histórico bruto)
        urls_vistas = set()
        links_finais = []
        for link_info in links_finais_brutos:
            if link_info['url_final'] not in urls_vistas:
                links_finais.append(link_info)
                urls_vistas.add(link_info['url_final'])
        print(f"✅ Etapa 2 concluída: {len(links_finais)} links únicos resolvidos com sucesso.")

        # filtra por histórico
        links_a_processar = [t for t in links_finais if t['url_final'] not in urls_historicas]
        removidos = len(links_finais) - len(links_a_processar)
        print(f"  ->  Filtragem: {removidos} links removidos por já estarem no histórico.")

        if not links_a_processar:
            print("\n✅ Nenhuma notícia nova para processar. Encerrando.")
            return

        # ETAPA 3: Extração de metadados em paralelo
        print(f"\n[ETAPA 3/4] Extraindo conteúdo de {len(links_a_processar)} NOVOS links...")
        dados_para_df = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXTRACAO) as executor:
            future_to_url = {
                executor.submit(extrair_conteudo_worker, tarefa['chave'], tarefa['url_final']): tarefa['url_final']
                for tarefa in links_a_processar
            }
            for i, future in enumerate(as_completed(future_to_url)):
                print(f"  - Progresso: [{i + 1}/{len(links_a_processar)}] Conteúdo extraído...")
                try:
                    resultado = future.result()
                    if resultado is None:
                        continue
                    dados_para_df.append(resultado)
                except Exception as e:
                    print(f"AVISO: Future falhou e será descartado ({type(e).__name__}: {e})")
                    continue

        print(f"✅ Etapa 3 concluída: {len(dados_para_df)} conteúdos extraídos com sucesso.")
        descartados = len(links_a_processar) - len(dados_para_df)
        print(f"  -> Descuidos/Inválidos descartados: {descartados}")

        # ETAPA 4: Salvamento do arquivo de análise do dia
        print(f"\n[ETAPA 4/4] Gerando e salvando o arquivo de análise de hoje...")
        if dados_para_df:
            import csv
            noticias_para_analise = pd.DataFrame(dados_para_df)
            noticias_para_analise.to_csv(
                NOME_ARQUIVO_SAIDA_DIARIA,
                index=False, sep=';', encoding='utf-8-sig',
                quoting=csv.QUOTE_MINIMAL  # previne 'linhas quebradas' com ';' nos campos
            )
            print(f"✅ Etapa 4 concluída: Arquivo '{NOME_ARQUIVO_SAIDA_DIARIA}' salvo com {len(noticias_para_analise)} novas notícias.")
        else:
            print("⚠️ Nenhuma notícia válida foi processada. Nada será salvo.")

    finally:
        for d in set(DRIVERS_CRIADOS):
            try:
                d.quit()
            except Exception:
                pass
        end_time = time.time()
        print(f"\n🏁 PROCESSO CONCLUÍDO em {end_time - start_time:.2f} segundos. 🏁")

if __name__ == "__main__":
    main()
