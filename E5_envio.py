from json import dumps
from httplib2 import Http
import pandas as pd
import time
import os
import csv

from dotenv import load_dotenv

load_dotenv() 

def enviar_noticia_para_chat(noticia):
    """
    Função que formata e envia uma única notícia para o Google Chat.
    """
    CHAT_WEBHOOK_URL_SAURON = os.getenv("CHAT_WEBHOOK_URL_SAURON")
    url_webhook = CHAT_WEBHOOK_URL_SAURON
    
    texto_mensagem = (
        f"🚨 *Alerta de Notícias* 🚨\n\n"
        f"A gestora: *{noticia['gestora'].upper()}* foi noticiada!\n\n"
        f"Descrição (gerada por IA) :_{noticia['descricao']}_\n\n"
        f"*{noticia['titulo']}*\n\n"
        f"Link: {noticia['url']}"
    )
    
    app_message = {"text": texto_mensagem}
    message_headers = {"Content-Type": "application/json; charset=UTF-8"}
    http_obj = Http()
    
    try:
        response, content = http_obj.request(
            uri=url_webhook,
            method="POST",
            headers=message_headers,
            body=dumps(app_message),
        )
        
        if response.status == 200:
            print(f"✅ Notícia sobre '{noticia['gestora']}' enviada com sucesso!")
        else:
            print(f"❌ Falha ao enviar notícia sobre '{noticia['gestora']}'. Status: {response.status}")
            print(f"   Detalhe: {content.decode('utf-8')}")
            
    except Exception as e:
        print(f"❌ Ocorreu um erro de conexão ao tentar enviar a notícia: {e}")

def main():
    caminho_arquivo = r'C:\Users\Arthur Braz\monitoramento_midia\noticias_para_envio.csv'
    colunas = [
        'gestora', 'titulo', 'subtitulo', 'url', 'alvo', 'classificacao',
        'interesse', 'resposta_modelo', 'texto', 'descricao', 'justificativa_alvo'
    ]

    try:
        # 1. Carrega os dados do CSV
        news_df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8-sig')
        
        # 2. Limpa a coluna alvo
        if 'alvo' in news_df.columns:
            news_df['alvo'] = news_df['alvo'].astype(str).str.strip().str.replace('"', '')
        else:
            print("⚠️ Coluna 'alvo' não encontrada no CSV.")
            return
        
        # 3. Filtra notícias para envio
        noticias_para_envio = news_df.query('alvo == "S"')
        
        if noticias_para_envio.empty:
            print("ℹ️ Nenhuma notícia marcada com 'S' para envio.")
        else:
            print(f"🚀 Encontrei {len(noticias_para_envio)} notícias para enviar. Iniciando disparos...")
            for _, noticia in noticias_para_envio.iterrows():
                enviar_noticia_para_chat(noticia)
                time.sleep(1)  # pausa de 1 segundo entre mensagens
        
        # 4. Após o envio, sobrescreve o arquivo mantendo apenas o cabeçalho
        pd.DataFrame(columns=colunas).to_csv(
            caminho_arquivo,
            index=False,
            sep=';',
            encoding='utf-8-sig',
            quoting=csv.QUOTE_MINIMAL
        )
        print(f"🧹 Arquivo '{caminho_arquivo}' limpo (mantido apenas o cabeçalho).")

    except FileNotFoundError:
        print(f"❌ ERRO: O arquivo não foi encontrado no caminho: {caminho_arquivo}")
    except Exception as e:
        print(f"❌ ERRO INESPERADO: {e}")

if __name__ == "__main__":
    main()
