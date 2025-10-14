import pandas as pd
from newspaper import Article, Config
import time
import sys

def extrair_noticia(url):
    """
    Extrai o texto principal de uma notícia a partir de uma URL.
    Retorna o texto da notícia em caso de sucesso.
    Retorna None em caso de falha no download ou se o texto estiver vazio.
    """
    # Validação básica da URL
    if pd.isna(url) or not str(url).strip():
        print(f"AVISO: URL inválida ou vazia: {url}")
        return None
        
    # Garantir que a URL tem protocolo
    url = str(url).strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    try:
        # Configuração para simular um navegador e evitar bloqueios
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        config = Config()
        config.browser_user_agent = user_agent
        config.request_timeout = 10

        article = Article(url, config=config)
        
        article.download()
        
        # Pequena pausa para não sobrecarregar os servidores
        time.sleep(0.5)
        
        article.parse()
        
        # Verifica se o texto extraído não é vazio
        if article.text and article.text.strip():
            return article.text.strip()
        else:
            print(f"AVISO: Nenhum texto encontrado para a URL: {url}")
            return None

    except Exception as e:
        print(f"FALHA ao processar a URL {url}: {e}")
        return None

# --- Código Principal ---

def main():
    # 1. Carregar os dados
    try:
        news = pd.read_csv(
            r'C:\Users\Arthur Braz\monitoramento_midia\noticias_para_analise.csv', 
            sep=';', 
            #names=['gestora', 'titulo', 'subtitulo', 'url', 'alvo', 'classificacao', 'interesse', 'resposta_modelo'],
            encoding='utf-8-sig'  # Tente 'latin1' se utf-8 não funcionar
        )
        print(f"Arquivo carregado com sucesso. Total de linhas: {len(news)}")
        
    except FileNotFoundError:
        print("ERRO: O arquivo 'noticias_para_analise.csv' não foi encontrado. Verifique o caminho.")
        return
    except Exception as e:
        print(f"ERRO ao carregar o arquivo: {e}")
        return

    # 2. Limpeza e Filtragem Inicial
    news['interesse'] = news['interesse'].str.replace('"', '')
    noticias_para_analise = news.query('interesse == "S"').copy()
    
    print(f"Notícias relevantes encontradas: {len(noticias_para_analise)}")
    
    if noticias_para_analise.empty:
        print("Nenhuma notícia relevante encontrada.")
        return

    # 3. Extração do texto
    print("Iniciando a extração dos textos das notícias...")
    print("(Isso pode levar alguns minutos dependendo do número de URLs)")
    
    # Aplicar a função com indicador de progresso
    total_urls = len(noticias_para_analise)
    textos = []
    
    for i, url in enumerate(noticias_para_analise['url'], 1):
        print(f"Processando {i}/{total_urls}: {url[:50]}...")
        texto = extrair_noticia(url)
        textos.append(texto)
    
    noticias_para_analise['texto'] = textos
    print("Extração finalizada.")

    # 4. Remover as linhas onde o texto não pôde ser extraído
    print(f"Linhas antes da limpeza: {len(noticias_para_analise)}")
    
    noticias_analise_final = noticias_para_analise.dropna(subset=['texto'])
    
    print(f"Linhas após a limpeza: {len(noticias_analise_final)}")
    print(f"URLs que falharam: {len(noticias_para_analise) - len(noticias_analise_final)}")

    # 5. Salvar o DataFrame final
    if not noticias_analise_final.empty:
        output_file = 'noticias_para_envio.csv'
        noticias_analise_final.to_csv(output_file, index=False, sep=';', encoding='utf-8-sig')
        print(f"Arquivo '{output_file}' salvo com sucesso.")
        print(f"Total de notícias processadas: {len(noticias_analise_final)}")
    else:
        print("Nenhuma notícia foi processada com sucesso. O arquivo de saída não foi gerado.")

if __name__ == "__main__":
    main()