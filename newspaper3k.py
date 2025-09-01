import pandas as pd
from newspaper import Article, Config

# --- Função Modificada ---
# Agora retorna None em caso de falha, em vez de uma string vazia.
# Isso facilita a remoção das linhas problemáticas no DataFrame.
def extrair_noticia(url):
    """
    Extrai o texto principal de uma notícia a partir de uma URL.
    Retorna o texto da notícia em caso de sucesso.
    Retorna None em caso de falha no download ou se o texto estiver vazio.
    """
    try:
        # Configuração para simular um navegador e evitar bloqueios
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
        config = Config()
        config.browser_user_agent = user_agent
        config.request_timeout = 10

        article = Article(url, config=config)
        
        article.download()
        article.parse()
        
        # Verifica se o texto extraído não é vazio (usando .strip() para remover espaços em branco)
        if article.text and article.text.strip():
            return article.text
        else:
            print(f"AVISO: Nenhum texto encontrado para a URL: {url}")
            return None # << ALTERAÇÃO 1: Retorna None se o texto for vazio

    except Exception as e:
        print(f"FALHA ao processar a URL {url}: {e}")
        return None # << ALTERAÇÃO 2: Retorna None em caso de qualquer exceção

# --- Código Principal ---

# 1. Carregar os dados
# Use um "raw string" (r'...') para evitar problemas com barras invertidas no caminho do Windows
try:
    news = pd.read_csv(r'C:\Users\Arthur Braz\monitoramento_midia\noticias_para_analise.csv', sep=';', names=['gestora', 'titulo', 'subtitulo', 'url', 'relevancia', 'alvo'])
except FileNotFoundError:
    print("ERRO: O arquivo 'noticias_para_analise.csv' não foi encontrado. Verifique o caminho.")
    exit() # Encerra o script se o arquivo não existir

# 2. Limpeza e Filtragem Inicial
news['relevancia'] = news['relevancia'].str.replace('"', '')
# Usar .copy() aqui evita um aviso comum do Pandas (SettingWithCopyWarning)
noticias_para_analise = news.query('relevancia == "S"').copy()

# 3. Extração do texto (isso pode levar um tempo dependendo do número de URLs)
print("Iniciando a extração dos textos das notícias...")
noticias_para_analise['texto'] = noticias_para_analise['url'].apply(extrair_noticia)
print("Extração finalizada.")

# --- A ALTERAÇÃO PRINCIPAL QUE VOCÊ PEDIU ---
# 4. Remover as linhas onde o texto não pôde ser extraído
print(f"Linhas antes da limpeza: {len(noticias_para_analise)}")

# O método .dropna() é a forma mais eficiente de remover linhas com valores None
noticias_analise_final = noticias_para_analise.dropna(subset=['texto'])

print(f"Linhas após a limpeza (apenas notícias com texto extraído): {len(noticias_analise_final)}")

# 5. Salvar o DataFrame final e limpo
if not noticias_analise_final.empty:
    noticias_analise_final.to_csv('noticias_para_envio.csv', index=False, sep=';')
    print("Arquivo 'noticias_para_envio.csv' salvo com sucesso.")
else:
    print("Nenhuma notícia foi processada com sucesso. O arquivo de saída não foi gerado.")