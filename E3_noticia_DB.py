import pandas as pd
from newspaper import Article, Config
import time
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed # NOVO: Para paralelismo

# --- CONFIGURAÇÕES DE DB E AMBIENTE (PADRÃO CI/CD) ---
load_dotenv()
DB_URL = os.getenv("DB_URL", "sqlite:///./data/noticias_pipeline.db")
TABLE_NAME = "noticias" 
MAX_WORKERS_EXTRACAO_TEXTO = int(os.getenv("MAX_WORKERS_E3", 10)) # NOVO: Aumento do paralelismo
SLEEP_PER_DOWNLOAD = float(os.getenv("SLEEP_E3", 0.1)) # NOVO: Reduzido para melhorar a velocidade (0.5s era muito)

# --- FUNÇÕES DE DB (REUTILIZADAS DA E2) ---

def get_db_engine():
    """Cria e retorna a engine do SQLAlchemy."""
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return engine
    except SQLAlchemyError as e:
        print(f"🚨 ERRO CRÍTICO: Falha na conexão com o Banco de Dados. Erro: {e}")
        raise RuntimeError("Falha na conexão com o DB.")

def load_relevant_unprocessed_news(engine: create_engine) -> pd.DataFrame:
    """
    Carrega notícias que foram classificadas como interesse='S' na E2 
    E que ainda não têm o campo 'texto' preenchido (ou seja, status_e2='CONCLUIDO').
    """
    print(f"Buscando notícias relevantes e sem texto na tabela '{TABLE_NAME}'...")
    # Filtro: Interesse='S' E Status E2='CONCLUIDO' (classificado) E Texto IS NULL (ainda não processado)
    query = f"""
    SELECT url
    FROM {TABLE_NAME}
    WHERE interesse = 'S' AND status_e2 = 'CONCLUIDO' AND texto IS NULL
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"🚨 ERRO ao carregar notícias do DB: {e}")
        raise

def update_news_text(engine: create_engine, data: list):
    """
    Atualiza as linhas do DB com o texto principal extraído.
    O 'data' deve ser uma lista de dicionários com 'url', 'texto' e 'status_e3'.
    """
    print(f"Iniciando atualização de {len(data)} linhas (texto principal) no DB...")
    
    # Prepara o comando SQL de UPDATE
    # O comando é parametrizado para evitar SQL Injection
    update_template = f"""
    UPDATE {TABLE_NAME}
    SET texto = :texto,
        status_e3 = 'CONCLUIDO'
    WHERE url = :url
    """
    try:
        with engine.begin() as connection:
            connection.execute(text(update_template), data)
        print(f"✅ {len(data)} linhas atualizadas com sucesso no DB (texto inserido).")
    except Exception as e:
        print(f"🚨 ERRO ao atualizar o DB (Texto): {e}")
        raise

# --- FUNÇÃO DE EXTRAÇÃO (ADAPTADA PARA PARALELISMO) ---

def extrair_noticia(url):
    """
    Extrai o texto principal de uma notícia a partir de uma URL.
    Retorna um dicionário {url: texto} ou {url: None} em caso de falha.
    """
    # Validação básica da URL
    if pd.isna(url) or not str(url).strip():
        print(f"AVISO: URL inválida ou vazia: {url}")
        return {'url': url, 'texto': None}
        
    url = str(url).strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Configuração para simular um navegador e evitar bloqueios
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
    config = Config()
    config.browser_user_agent = user_agent
    config.request_timeout = 10
    
    texto_extraido = None
    
    try:
        article = Article(url, config=config)
        
        article.download()
        
        # Pausa reduzida e externalizada para controle do ambiente/CI/CD
        time.sleep(SLEEP_PER_DOWNLOAD)
        
        article.parse()
        
        if article.text and article.text.strip():
            texto_extraido = article.text.strip()
        else:
            print(f"AVISO: Nenhum texto encontrado para a URL: {url[:60]}...")
            
    except Exception as e:
        print(f"FALHA ao processar a URL {url[:60]}...: {e}")
        
    # Retorna o dicionário de resultados para atualização do DB
    return {'url': url, 'texto': texto_extraido, 'status_e3': 'CONCLUIDO' if texto_extraido else 'FALHA'}


# --- MAIN - FLUXO ORQUESTRADO ---

def main():
    start_time = time.time()
    print("🚀 E3 - INICIANDO EXTRAÇÃO DE CONTEÚDO (NEWSPAPER) 🚀")
    
    DB_ENGINE = get_db_engine()
    
    # 1. Carregar os dados (Notícias Relevantes e sem texto)
    df_pendente = load_relevant_unprocessed_news(DB_ENGINE)
    total_urls = len(df_pendente)
    
    print(f"✅ {total_urls} notícias relevantes e sem texto carregadas do DB.")
    
    if total_urls == 0:
        print("✅ Nenhuma notícia nova e relevante para extrair texto. Encerrando E3.")
        return

    # 2. Extração do texto em Paralelo
    print(f"\n[ETAPA 2/3] Iniciando a extração paralela dos textos com {MAX_WORKERS_EXTRACAO_TEXTO} workers...")
    
    urls_a_processar = df_pendente['url'].tolist()
    resultados_finais = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXTRACAO_TEXTO) as executor:
        # Mapeia cada URL para a função extrair_noticia
        futures = {executor.submit(extrair_noticia, url): url for url in urls_a_processar}
        
        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                resultados_finais.append(result)
                print(f"[Progresso: {i}/{total_urls}] Processado: {result['url'][:50]}...")
            except Exception as e:
                print(f"AVISO: Thread de extração falhou: {e}")
                
    print("Extração paralela finalizada.")

    # 3. Preparação e Atualização do Banco de Dados
    
    # Remove resultados que falharam (texto é None) para não tentar salvar lixo no DB
    # Nota: A atualização do status 'FALHA' é importante para não reprocessar na próxima execução
    dados_para_db = []
    textos_nao_encontrados = 0
    
    for r in resultados_finais:
        # Colocamos o status de falha/concluído na lista para atualizar o DB
        if r['texto']:
            dados_para_db.append({'url': r['url'], 'texto': r['texto'], 'status_e3': 'CONCLUIDO'})
        else:
            dados_para_db.append({'url': r['url'], 'texto': None, 'status_e3': 'FALHA'})
            textos_nao_encontrados += 1

    print(f"URLs que falharam ou retornaram texto vazio: {textos_nao_encontrados}")
    
    # 4. Atualização do Banco de Dados
    if dados_para_db:
        update_news_text(DB_ENGINE, dados_para_db)
        print(f"Total de notícias com texto inserido: {len(dados_para_db) - textos_nao_encontrados}")
    else:
        print("Nenhuma notícia foi extraída com sucesso para atualização do DB.")
    
    print("🏁 PROCESSO E3 CONCLUÍDO. O DB está pronto para a Etapa 4. 🏁")
    end_time = time.time()
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos.")


if __name__ == "__main__":
    main()