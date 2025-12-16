from json import dumps
from httplib2 import Http
import pandas as pd
import time
import os
import csv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv

# --- CONFIGURAÇÕES DE DB E AMBIENTE (PADRÃO CI/CD) ---
load_dotenv() 
DB_URL = os.getenv("DB_URL", "sqlite:///./data/noticias_pipeline.db")
TABLE_NAME = "noticias" 
CHAT_WEBHOOK_URL_SAURON = os.getenv("CHAT_WEBHOOK_URL_SAURON")
MAX_WORKERS_CHAT = int(os.getenv("MAX_WORKERS_CHAT", 4)) # Paralelismo para envio de alertas
SLEEP_PER_SEND = float(os.getenv("SLEEP_E5", 1.0)) # Pausa para evitar rate limit do Google Chat

# --- FUNÇÕES DE DB ---

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

def load_ready_to_send_news(engine: create_engine) -> pd.DataFrame:
    """Carrega notícias prontas (Alvo='S') e que ainda não foram enviadas (E5=PENDENTE)."""
    print(f"Buscando notícias prontas para envio (Alvo='S', E4=CONCLUIDO, E5=PENDENTE)...")
    
    # Filtro: Alvo='S' AND status_e4='CONCLUIDO' AND status_e5 IS NULL/PENDENTE
    query = f"""
    SELECT url, gestora, titulo, descricao
    FROM {TABLE_NAME}
    WHERE alvo = 'S' 
      AND status_e4 = 'CONCLUIDO'
      AND (status_e5 IS NULL OR status_e5 = 'PENDENTE')
    """
    try:
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"🚨 ERRO ao carregar notícias do DB para envio: {e}")
        raise

def update_news_status_e5(engine: create_engine, results: list):
    """Atualiza o status E5 de todas as URLs processadas, marcando sucesso ou falha."""
    print(f"Iniciando atualização de {len(results)} linhas (Status E5) no DB...")
    
    # Prepara o comando SQL de UPDATE
    update_template = f"""
    UPDATE {TABLE_NAME}
    SET status_e5 = :status_e5,
        msg_e5_erro = :msg_e5_erro
    WHERE url = :url
    """
    try:
        with engine.begin() as connection:
            # results deve ser uma lista de dicts com {'url', 'status_e5', 'msg_e5_erro'}
            connection.execute(text(update_template), results)
        print(f"✅ {len(results)} linhas de status E5 atualizadas com sucesso.")
    except Exception as e:
        print(f"🚨 ERRO ao atualizar o DB (Status E5): {e}")
        raise

# --- FUNÇÕES DE ENVIO (ADAPTADAS PARA TRABALHAR COM O DB) ---

def enviar_noticia_worker(noticia: pd.Series):
    """
    Worker que formata e envia uma única notícia para o Google Chat.
    Retorna o status do envio.
    """
    url_webhook = CHAT_WEBHOOK_URL_SAURON
    
    if not url_webhook:
        return {'url': noticia['url'], 'status_e5': 'FALHA', 'msg_e5_erro': 'WEBHOOK_NAO_CONFIGURADO'}

    if noticia['gestora'] in ['Xp Investimentos', 'Vinci', 'Tivio','Tarpon','Bnp', 'Oceana']:
        tipo_getora = '(fundos exclusivos)'   
        texto_mensagem = (
            f"🚨 *Alerta de Notícias* 🚨\n\n"
            f"A gestora: *{noticia['gestora'].upper()}* foi noticiada! _{tipo_getora}_\n\n"
            f"_Descrição (gerada por IA)_ :{noticia['descricao']}\n\n"
            f"*{noticia['titulo']}*\n\n"
            f"Link: {noticia['url']}"
        )
    else:
        tipo_getora = '(fundos não-exclusivos)'   
        texto_mensagem = (
            f"🚨 *Alerta de Notícias* 🚨\n\n"
            f"A gestora: *{noticia['gestora'].upper()}* foi noticiada! _{tipo_getora}_\n\n"
            f"_Descrição (gerada por IA)_ :{noticia['descricao']}\n\n"
            f"*{noticia['titulo']}*\n\n"
            f"Link: {noticia['url']}" )
        
    app_message = {"text": texto_mensagem}
    message_headers = {"Content-Type": "application/json; charset=UTF-8"}
    http_obj = Http()
    
    status_e5 = 'FALHA'
    msg_e5_erro = None
    
    try:
        response, content = http_obj.request(
            uri=url_webhook,
            method="POST",
            headers=message_headers,
            body=dumps(app_message),
        )
        
        if response.status == 200:
            print(f"✅ Notícia sobre '{noticia['gestora']}' enviada com sucesso!")
            status_e5 = 'ENVIADO'
        else:
            error_content = content.decode('utf-8')
            print(f"❌ Falha ao enviar '{noticia['gestora']}'. Status: {response.status}. Detalhe: {error_content}")
            status_e5 = 'FALHA_CHAT'
            msg_e5_erro = f"{response.status}: {error_content[:50]}"
            
    except Exception as e:
        print(f"❌ Ocorreu um erro de conexão/HTTP ao enviar a notícia: {e}")
        status_e5 = 'FALHA_HTTP'
        msg_e5_erro = str(e)
        
    return {'url': noticia['url'], 'status_e5': status_e5, 'msg_e5_erro': msg_e5_erro}

# --- MAIN - FLUXO ORQUESTRADO ---

def main():
    start_time = time.time()
    print("🚀 E5 - INICIANDO ENVIO DE ALERTAS PARA CHAT (CD) 🚀")
    
    # 1. Conexão e Carregamento de Dados
    DB_ENGINE = get_db_engine()
    df_pendente = load_ready_to_send_news(DB_ENGINE)
    
    total = len(df_pendente)
    print(f"✅ {total} notícias prontas (alvo=S) pendentes de envio.")

    if total == 0:
        print("✅ Nenhuma notícia pronta para alerta. Encerrando E5.")
        return
        
    # 2. Envio Paralelo
    print(f"\n[ETAPA 2/3] Iniciando envio paralelo de alertas com {MAX_WORKERS_CHAT} workers...")
    
    # O Google Chat tem limites rigorosos, o paralelismo é pequeno e a pausa é mantida
    resultados_envio = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHAT) as executor:
        futures = {executor.submit(enviar_noticia_worker, row): row['url'] 
                   for _, row in df_pendente.iterrows()}
        
        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                resultados_envio.append(result)
                print(f"[Progresso: {i}/{total}] Envio concluído para: {result['url'][:50]}...")
            except Exception as e:
                print(f"AVISO: Thread de envio falhou: {e}")
                
            # Pausa para evitar rate limit no Google Chat
            time.sleep(SLEEP_PER_SEND) 

    print("Envio paralelo finalizado.")

    # 3. Atualização do Banco de Dados
    if resultados_envio:
        update_news_status_e5(DB_ENGINE, resultados_envio)
    
    # A lógica de limpeza do CSV foi removida, pois o status_e5 no DB marca o item como 'concluído'.
    
    print("🏁 PROCESSO E5 CONCLUÍDO. O pipeline de Alerta está completo. 🏁")
    end_time = time.time()
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos.")


if __name__ == "__main__":
    main()