#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

import feedparser
import requests
from dotenv import load_dotenv 
from groq import Groq # SDK Groq para chamadas mais robustas

# --- Configuração de Ambiente (CI/CD) -----------------------------------------
load_dotenv() 

RSS_URL = "https://www.google.com.br/alerts/feeds/09404460482838700245/11335182104059770088"

# PADRÃO CI/CD: Caminho do DB persistente na pasta 'data'
DB_FILENAME = "sent_links.db"
DB_DIR = os.environ.get("DATA_DIR", "./data") 
DB_PATH = os.path.join(DB_DIR, DB_FILENAME)

GROQ_MODEL = os.environ.get("GROQ_MODEL", "mixtral-8x7b-32768") # Modelo LLM
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
GOOGLE_CHAT_WEBHOOK_URL = os.environ.get("CHAT_WEBHOOK_URL_HALL")

TIMEOUT = (10, 30) # Timeout para requisições
API_SLEEP = float(os.environ.get("E7_API_SLEEP", 1.0)) # Pausa após chamada da Groq

# --- Utilidades ---------------------------------------------------------------

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s - %(message)s"
)

def db_init(db_path: str = DB_PATH):
    """
    Cria (se não existir) a conexão e a tabela do DB.
    Garante que o diretório do DB exista.
    """
    # Garante que a pasta 'data' exista (padrão de montagem de volume)
    os.makedirs(os.path.dirname(db_path), exist_ok=True) 

    con = sqlite3.connect(db_path)
    con.execute(
        """
            CREATE TABLE IF NOT EXISTS sent_links (
            link TEXT PRIMARY KEY,
            sent_at TEXT NOT NULL
        )
    """
    )
    con.commit()
    return con

def already_sent(con: sqlite3.Connection, link: str) -> bool:
    cur = con.execute("SELECT 1 FROM sent_links WHERE link = ?", (link,))
    return cur.fetchone() is not None

def mark_sent(con: sqlite3.Connection, link: str):
    con.execute(
         "INSERT OR IGNORE INTO sent_links(link, sent_at) VALUES(?, ?)",
         (link, datetime.now(timezone.utc).isoformat())
    )
    con.commit()

def fetch_feed(url: str):
    return feedparser.parse(url)

def pick_latest_unsent(feed, con):
    entries = feed.entries
    def sort_key(e):
        return getattr(e, "published_parsed", None) or time.gmtime(0)
    entries_sorted = sorted(entries, key=sort_key, reverse=True)
    for e in entries_sorted:
        link = getattr(e, "link", None)
        if link and not already_sent(con, link):
             return e
    return None

def groq_summarize(text: str, title: str = "") -> str:
    if not GROQ_API_KEY:
     raise RuntimeError("GROQ_API_KEY não definido no .env")

    client = Groq(api_key=GROQ_API_KEY) 

    prompt = (
        "Você é um assistente que resume notícias em português do Brasil.\n"
        "Crie um resumo conciso (~400 caracteres, variação ±10%), claro e informativo, "
        "sem hashtags, emojis ou opinião. Inclua apenas fatos e contexto essencial. Vale ressaltar "
        "que vocÊ fará o resumo se a matéria for sobre a EFPC Ceres Previdência, se não for sobre ela retorne \n\n"
        f"Título: {title}\n"
        f"Conteúdo:\n{text}\n\n"
        "Resumo:"
    )

    messages = [
        {"role": "system", "content": "Você escreve resumos jornalísticos precisos em pt-BR."},
        {"role": "user", "content": prompt},
    ]
    
    r = client.chat.completions.create(
        model=GROQ_MODEL,
        temperature=0.2,
        messages=messages,
        timeout=TIMEOUT[1]
)
    
    summary = r.choices[0].message.content.strip()
    if len(summary) > 520:
        summary = summary[:520].rstrip() + "…"
    return summary

def build_message(title: str, link: str, summary: str) -> str:
    """ Monta a mensagem final que será enviada ao Google Chat. """
    return (
        f"✳️✳️ A *Ceres* foi noticiada! ✳️✳️\n\n"
        f"*Resumo:* {summary}\n\n"
        f"*Link:* <{link}|Clique para ler>"
    )


def send_to_google_chat(webhook_url: str, text: str):
    if not webhook_url:
        raise RuntimeError("GOOGLE_CHAT_WEBHOOK_URL não definido no .env")
    r = requests.post(webhook_url, json={"text": text}, timeout=TIMEOUT)
    r.raise_for_status()


# --- Principal ---------------------------------------------------------------

def main():
    logging.info("Lendo RSS…")

    con = None 
    
    try:
        con = db_init()
        feed = fetch_feed(RSS_URL)

        if not feed.entries:
            logging.warning("Nenhuma entrada encontrada no RSS.")
            return

        entry = pick_latest_unsent(feed, con)
        if not entry:
            logging.info("Nenhuma notícia nova para enviar.")
            return

        title = getattr(entry, "title", "Sem título")
        link = getattr(entry, "link", "")
        summary_source = getattr(entry, "summary", "") or getattr(entry, "description", "") or title

        logging.info("Gerando resumo com Groq…")
        try:
            resumo = groq_summarize(summary_source, title=title)
        except Exception as e:
            # Em caso de falha da API (ex: RateLimit), usa o summary bruto
            logging.exception("Falha no resumo via Groq. Usando resumo bruto.")
            resumo = (summary_source or title)[:400].strip() + "…"

        # Pausa de controle para evitar Rate Limit
        time.sleep(API_SLEEP)
        
        texto = build_message(title, link, resumo)

        logging.info("Enviando para Google Chat…")
        send_to_google_chat(GOOGLE_CHAT_WEBHOOK_URL, texto)

        mark_sent(con, link)
        logging.info("Concluído. Link marcado como enviado.")

    except RuntimeError as e:
        # Erros críticos de configuração (Webhook, API Key)
        logging.error(f"Erro Crítico de Configuração: {e}")
    except requests.exceptions.RequestException as e:
        # Erros de rede/HTTP (falha ao acessar RSS, Groq ou Chat)
        logging.error(f"Erro de Rede/API: Falha ao comunicar com um serviço externo: {e}")
    except Exception as e:
        # Erros inesperados
        logging.exception("Ocorreu um erro fatal e não tratado.")

    finally:
        if con:
            con.close()


if __name__ == "__main__":
    setup_logging()
    main()