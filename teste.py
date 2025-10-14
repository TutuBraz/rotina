#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import re
import html
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
import feedparser
import pandas as pd

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

# Configurações
MAX_WORKERS_FEEDS = 8
NOME_ARQUIVO_HISTORICO = r'C:\Users\Arthur Braz\monitoramento_midia\noticias_para_historico.csv'
NOME_ARQUIVO_SAIDA_DIARIA = 'noticias_para_analise.csv'

# Headers para requests
HEADERS = {"User-Agent": "Mozilla/5.0 (feed-fetcher/1.0)"}

# Configurações de filtros
MIN_TITULO_CHARS = 8
MIN_DESCRICAO_CHARS = 10
BLACKLIST_TITULOS = {"home", "login", "index of", "redirecting", "oops", "error", "403", "404"}

def clean_text(text):
    """Remove HTML tags e limpa o texto"""
    if not text:
        return ""
    
    # Remove tags HTML
    text = re.sub(r'<[^>]+>', '', text)
    # Decodifica entidades HTML
    text = html.unescape(text)
    # Remove quebras de linha excessivas
    text = re.sub(r'\n+', ' ', text)
    # Remove espaços excessivos
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def get_with_retries(url: str, tries: int = 3, backoff_base: float = 2.0):
    """GET com retries exponenciais simples."""
    for i in range(tries):
        try:
            return requests.get(url, headers=HEADERS, timeout=(5, 15), allow_redirects=True)
        except Exception:
            if i == tries - 1:
                raise
            sleep_for = (backoff_base ** i) + (0.1 * i)
            time.sleep(sleep_for)

def processar_feed_rss(gestora, feed_url):
    """Processa um feed RSS e extrai título e descrição de cada entrada"""
    try:
        print(f"    · Processando feed {gestora}: {feed_url[:70]}...")
        
        # Baixa o feed
        response = get_with_retries(feed_url, tries=3)
        response.raise_for_status()
        
        # Parse do feed
        feed = feedparser.parse(response.content)
        
        if not hasattr(feed, 'entries') or not feed.entries:
            print(f"    · AVISO: Nenhuma entrada encontrada no feed {gestora}")
            return []
        
        noticias = []
        for entry in feed.entries:
            # Extrai dados básicos do entry
            titulo = clean_text(entry.get('title', ''))
            descricao = clean_text(entry.get('summary', '') or entry.get('description', ''))
            link = entry.get('link', '')
            
            # Data de publicação
            pub_date = None
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                try:
                    pub_date = datetime(*entry.published_parsed[:6]).strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pub_date = entry.get('published', '')
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                try:
                    pub_date = datetime(*entry.updated_parsed[:6]).strftime("%Y-%m-%d %H:%M:%S")
                except:
                    pub_date = entry.get('updated', '')
            
            # Aplica filtros de qualidade
            if not validar_noticia(titulo, descricao, link):
                continue
            
            noticia = {
                'gestora': gestora,
                'titulo': titulo,
                'subtitulo': descricao,  # mantendo o nome da coluna original
                'url': link,
                'data_publicacao': pub_date,
                'alvo': None,
                'classificacao': None,
                'interesse': None,
                'resposta_modelo': None,
                'texto': None,
                'descricao': None,
                'justificativa_alvo': None
            }
            
            noticias.append(noticia)
        
        print(f"    · ✅ {len(noticias)} notícias válidas extraídas de {gestora}")
        return noticias
        
    except Exception as e:
        print(f"    · ❌ Erro ao processar feed {gestora}: {e}")
        return []

def validar_noticia(titulo, descricao, link):
    """Valida se a notícia atende aos critérios mínimos de qualidade"""
    
    # Verifica se tem título
    if not titulo or len(titulo) < MIN_TITULO_CHARS:
        return False
    
    # Verifica se tem link
    if not link or not link.startswith(('http://', 'https://')):
        return False
    
    # Verifica títulos genéricos/inválidos
    titulo_lower = titulo.lower()
    if any(blacklist in titulo_lower for blacklist in BLACKLIST_TITULOS):
        return False
    
    # Verifica se tem conteúdo mínimo (título + descrição)
    conteudo_total = f"{titulo} {descricao}".strip()
    if len(conteudo_total) < 20:  # muito pouco conteúdo
        return False
    
    # Verifica padrões de erro/bloqueio
    texto_completo = conteudo_total.lower()
    padroes_erro = [
        "access denied", "403 forbidden", "404 not found", 
        "just a moment", "checking your browser", "captcha",
        "temporarily unavailable", "service unavailable"
    ]
    
    if any(padrao in texto_completo for padrao in padroes_erro):
        return False
    
    return True

def coletar_todas_noticias(default_feeds, max_workers=8):
    """Coleta notícias de todos os feeds RSS em paralelo"""
    todas_noticias = []
    
    # Prepara lista de tarefas
    tarefas = []
    for gestora, feeds in default_feeds.items():
        for feed_url in feeds:
            tarefas.append((gestora, feed_url))
    
    # Executa em paralelo
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_tarefa = {
            executor.submit(processar_feed_rss, gestora, feed_url): (gestora, feed_url)
            for gestora, feed_url in tarefas
        }
        
        for future in as_completed(future_to_tarefa):
            gestora, feed_url = future_to_tarefa[future]
            try:
                noticias = future.result()
                todas_noticias.extend(noticias)
            except Exception as e:
                print(f"    · ❌ Erro ao processar {gestora}: {e}")
    
    return todas_noticias

def remover_duplicatas(noticias):
    """Remove notícias duplicadas com base na URL"""
    urls_vistas = set()
    noticias_unicas = []
    
    for noticia in noticias:
        url = noticia.get('url', '')
        if url and url not in urls_vistas:
            urls_vistas.add(url)
            noticias_unicas.append(noticia)
    
    duplicatas_removidas = len(noticias) - len(noticias_unicas)
    if duplicatas_removidas > 0:
        print(f"    · 🔄 Removidas {duplicatas_removidas} notícias duplicadas")
    
    return noticias_unicas

def main():
    """Função principal do monitoramento"""
    start_time = time.time()
    print("🚀 INICIANDO MONITORAMENTO RSS SIMPLIFICADO 🚀")
    
    try:
        # ETAPA 1: Carregar histórico
        print(f"\n[ETAPA 1/4] Carregando histórico de '{NOME_ARQUIVO_HISTORICO}'...")
        urls_historicas = set()
        
        try:
            df_historico = pd.read_csv(NOME_ARQUIVO_HISTORICO, sep=';', encoding='utf-8-sig')
            if 'url' in df_historico.columns:
                urls_historicas = set(df_historico['url'].dropna().astype(str))
                print(f"✅ {len(urls_historicas)} URLs encontradas no histórico")
            else:
                print("⚠️  Coluna 'url' não encontrada no histórico")
        except FileNotFoundError:
            print(f"⚠️  Arquivo de histórico não encontrado: {NOME_ARQUIVO_HISTORICO}")
        except Exception as e:
            print(f"❌ Erro ao carregar histórico: {e}")
        
        # ETAPA 2: Coletar notícias dos feeds RSS
        print(f"\n[ETAPA 2/4] Coletando notícias dos feeds RSS...")
        todas_noticias = coletar_todas_noticias(DEFAULT_FEEDS, max_workers=MAX_WORKERS_FEEDS)
        print(f"✅ {len(todas_noticias)} notícias coletadas no total")
        
        if not todas_noticias:
            print("⚠️ Nenhuma notícia foi coletada. Encerrando.")
            return
        
        # ETAPA 3: Processar e filtrar notícias
        print(f"\n[ETAPA 3/4] Processando e filtrando notícias...")
        
        # Remove duplicatas
        noticias_unicas = remover_duplicatas(todas_noticias)
        
        # Filtra por histórico
        noticias_novas = [
            noticia for noticia in noticias_unicas 
            if noticia.get('url', '') not in urls_historicas
        ]
        
        removidas_historico = len(noticias_unicas) - len(noticias_novas)
        print(f"    · 📚 Removidas {removidas_historico} notícias já existentes no histórico")
        print(f"✅ {len(noticias_novas)} notícias novas para análise")
        
        # ETAPA 4: Salvar resultado
        print(f"\n[ETAPA 4/4] Salvando arquivo de análise...")
        
        if noticias_novas:
            df_noticias = pd.DataFrame(noticias_novas)
            df_noticias.to_csv(NOME_ARQUIVO_SAIDA_DIARIA, index=False, sep=';', encoding='utf-8-sig')
            print(f"✅ Arquivo '{NOME_ARQUIVO_SAIDA_DIARIA}' salvo com {len(noticias_novas)} notícias")
            
            # Resumo por gestora
            print("\n📊 RESUMO POR GESTORA:")
            resumo = df_noticias.groupby('gestora').size().sort_values(ascending=False)
            for gestora, count in resumo.items():
                print(f"    · {gestora}: {count} notícias")
                
        else:
            print("⚠️ Nenhuma notícia nova encontrada. Nada a salvar.")
    
    except Exception as e:
        print(f"❌ Erro crítico no processo: {e}")
        raise
    
    finally:
        end_time = time.time()
        print(f"\n🏁 PROCESSO CONCLUÍDO em {end_time - start_time:.2f} segundos 🏁")

if __name__ == "__main__":
    main()