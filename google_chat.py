from json import dumps
from httplib2 import Http
import pandas as pd
import time # Importado para adicionar um pequeno intervalo
import os

from dotenv import load_dotenv

load_dotenv() 

def enviar_noticia_para_chat(noticia):
    """
    Função que formata e envia uma única notícia para o Google Chat.
    """
    # ATENÇÃO: Nunca exponha esta URL publicamente (ex: em repositórios de código).
    # O ideal é armazená-la de forma segura, como em variáveis de ambiente.
    CHAT_WEBHOOK_URL_SAURON = os.getenv("CHAT_WEBHOOK_URL_SAURON")

    url_webhook = CHAT_WEBHOOK_URL_SAURON
    
    # Formata a mensagem dinamicamente usando os dados da notícia
    texto_mensagem = (
        f"🚨 *Alerta de Notícias* 🚨\n\n"
        f"A gestora: *{noticia['gestora'].upper()}* foi noticiada!\n\n"
        f"*{noticia['titulo']}*\n"
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
        
        # Verifica se a mensagem foi enviada com sucesso (status 200)
        if response.status == 200:
            print(f"✅ Notícia sobre '{noticia['gestora']}' enviada com sucesso!")
        else:
            print(f"❌ Falha ao enviar notícia sobre '{noticia['gestora']}'. Status: {response.status}")
            print(f"   Detalhe: {content.decode('utf-8')}")
            
    except Exception as e:
        print(f"❌ Ocorreu um erro de conexão ao tentar enviar a notícia: {e}")

def main():
    """
    Lê o arquivo CSV, filtra as notícias relevantes e chama a função de envio para cada uma.
    """
    try:
        # 1. Carrega os dados do CSV.
        #    Certifique-se que o caminho está correto e que o arquivo não tem cabeçalho.
        caminho_arquivo = r'C:\Users\Arthur Braz\monitoramento_midia\noticias_para_envio.csv'
        colunas = ['gestora', 'titulo', 'subtitulo', 'url', 'relevancia', 'alvo', 'texto']
        news_df = pd.read_csv(caminho_arquivo, sep=';', names=colunas, header=None)
        
        # 2. Limpa os dados da coluna 'alvo' para garantir uma filtragem correta.
        #    Remove espaços em branco e aspas.
        news_df['alvo'] = news_df['alvo'].str.strip().str.replace('"', '')
        
        # 3. Filtra o DataFrame para pegar apenas as notícias marcadas com "S".
        noticias_para_envio = news_df.query('alvo == "S"')
        
        if noticias_para_envio.empty:
            print("ℹ️ Nenhuma notícia marcada com 'S' para envio.")
            return

        print(f"🚀 Encontrei {len(noticias_para_envio)} notícias para enviar. Iniciando disparos...")
        
        # 4. Itera sobre cada linha do DataFrame filtrado e envia a notícia.
        for index, noticia in noticias_para_envio.iterrows():
            enviar_noticia_para_chat(noticia)
            time.sleep(1) # Adiciona uma pausa de 1 segundo para não sobrecarregar a API.

    except FileNotFoundError:
        print(f"❌ ERRO: O arquivo não foi encontrado no caminho: {caminho_arquivo}")
    except Exception as e:
        print(f"❌ ERRO INESPERADO: Ocorreu um problema ao processar o arquivo. Detalhe: {e}")

if __name__ == "__main__":
    main()