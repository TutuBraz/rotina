import os
import time
import logging
import google.generativeai as genai
from json import dumps
from httplib2 import Http
from newspaper import Article
from dotenv import load_dotenv

# ==================== CONFIGURAÇÃO ====================

def setup_logging():
    """Configura o logging para exibir mensagens formatadas no console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)

# Carrega as variáveis de ambiente (coloque em um arquivo .env)
load_dotenv()

# ==================== DADOS PARA TESTE ====================
#
# Edite esta lista para testar as URLs que você quiser.
# Para cada URL, especifique a 'gestora' que a IA deve procurar no texto.
#
urls_para_testar = [
    {
        "gestora": "XP Investimentos",
        "url": "https://g1.globo.com/economia/educacao-financeira/noticia/2025/04/26/vazamento-de-dados-da-xp-entenda-quais-os-cuidados-necessarios-para-evitar-golpes.ghtml"
    },
    {
        "gestora": "Tivio Capital",
        "url": "https://einvestidor.estadao.com.br/mercado/administradora-fii-bb-milhoes-cvm-conflito-de-interesses-falhas-em-informacoes/"
    },
    {
        "gestora": "BNP Paribas",
        "url": "https://g1.globo.com/mundo/noticia/banco-bnp-paribas-sera-processado-por-seu-papel-no-genocidio-em-ruanda.ghtml" 
    }
]


# ==================== FUNÇÕES PRINCIPAIS ====================

def configurar_modelo_ia() -> genai.GenerativeModel:
    """Configura e retorna o modelo generativo do Gemini."""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("Chave de API do Gemini não encontrada. Verifique o arquivo .env.")
    
    genai.configure(api_key=api_key)
    
    system_instruction = """
    Você atuará como um Analista Sênior de Risco Reputacional. Sua tarefa é analisar uma notícia e classificar o risco para uma GESTORA ALVO específica.

    **Regras de Análise:**
    1.  **Foco na Gestora Alvo:** A análise deve se concentrar exclusivamente na GESTORA ALVO. Se a gestora for mencionada apenas de forma secundária (ex: como fonte de um comentário), o risco deve ser classificado como "Nenhum".
    2.  **Classificação do Fator de Risco:** Classifique o evento em UMA das seguintes categorias:
        *   'Integridade e Ética'
        *   'Fiduciário e de Produto'
        *   'Operacional e de Segurança'
        *   'Governança e Liderança'
        *   'Legal e Regulatório'
        *   'ESG e Social'
        *   'Risco de Terceiros'
        *   'Nenhum'

    **Formato da Resposta:**
    Sua resposta final deve ser **APENAS UMA** das opções acima, sem explicações.
    """
    
    return genai.GenerativeModel(
        model_name="gemini-2.0-flash",
        system_instruction=system_instruction
    )

def extrair_texto_do_artigo(url: str) -> dict or None:
    """Usa newspaper3k para extrair título e texto de uma URL."""
    try:
        logging.info(f"Tentando extrair conteúdo de: {url}")
        article = Article(url)
        article.download()
        article.parse()
        
        if not article.text or len(article.text.strip()) < 100:
            logging.warning("Conteúdo vazio ou insuficiente.")
            return None
            
        return {
            "titulo": article.title,
            "texto": article.text,
            "fonte": article.source_url
        }
    except Exception as e:
        logging.error(f"Falha ao processar a URL {url}: {e}")
        return None

def analisar_risco(modelo_ia: genai.GenerativeModel, gestora: str, texto: str) -> str:
    """Envia o texto para a IA e retorna a classificação de risco."""
    prompt = f"""
    GESTORA ALVO: {gestora}
    ---
    NOTÍCIA PARA ANÁLISE:
    ---
    {texto}
    """
    try:
        response = modelo_ia.generate_content(prompt)
        risco = response.text.strip().lower().replace("'", "") # Remove aspas
        return risco
    except Exception as e:
        logging.error(f"Falha na chamada da API do Gemini: {e}")
        return "erro_api"

def enviar_alerta_chat(gestora: str, titulo: str, link: str, risco: str, fonte: str):
    """Envia um alerta formatado para o Google Chat via Webhook."""
    chat_webhook_url = os.getenv("CHAT_WEBHOOK_URL_SAURON")
    if not chat_webhook_url:
        logging.warning("Webhook do Chat não configurado. O alerta não será enviado.")
        return

    app_message = {
        "text": f"🚨 *Alerta de Monitoramento (TESTE)* 🚨\n\nA gestora *{gestora.upper()}* foi noticiada!\n\nFator de risco: *{risco.upper()}*.\n\n*Notícia:* {titulo}\n*Fonte*: {fonte}\n*Link:* <{link}|Clique para ler>"
    }
    
    http_obj = Http()
    try:
        response, content = http_obj.request(
            uri=chat_webhook_url,
            method="POST",
            headers={"Content-Type": "application/json; charset=UTF-8"},
            body=dumps(app_message),
        )
        if response.status == 200:
            logging.info(f"Alerta sobre '{gestora}' enviado com sucesso para o Chat.")
        else:
            logging.error(f"Falha ao enviar alerta. Status: {response.status}, Resposta: {content.decode('utf-8')}")
    except Exception as e:
        logging.error(f"Exceção ao enviar alerta para o chat: {e}")


# ==================== EXECUÇÃO DO PROTÓTIPO ====================

if __name__ == "__main__":
    setup_logging()
    
    logging.info("Iniciando protótipo de análise e alerta...")
    
    try:
        modelo_gemini = configurar_modelo_ia()
        
        for teste in urls_para_testar:
            url = teste["url"]
            gestora = teste["gestora"]
            
            print("-" * 50)
            logging.info(f"Processando URL para a gestora '{gestora}'")
            
            # 1. Extrair conteúdo
            artigo_data = extrair_texto_do_artigo(url)
            
            if not artigo_data:
                continue
            
            # 2. Analisar o risco com a IA
            risco_classificado = analisar_risco(
                modelo_ia=modelo_gemini,
                gestora=gestora,
                texto=artigo_data["texto"]
            )
            logging.info(f"IA classificou o risco como: '{risco_classificado.upper()}'")
            
            # 3. Enviar alerta se o risco for relevante
            riscos_para_ignorar = ['nenhum', 'erro_api', 'sem_info']
            if risco_classificado not in riscos_para_ignorar:
                enviar_alerta_chat(
                    gestora=gestora,
                    titulo=artigo_data["titulo"],
                    link=url,
                    risco=risco_classificado,
                    fonte=artigo_data["fonte"]
                )
            else:
                logging.info("Nenhum risco relevante identificado. Alerta não enviado.")

            # Pausa para não sobrecarregar as APIs
            time.sleep(2)
            
        print("-" * 50)
        logging.info("Processo de teste concluído.")

    except Exception as e:
        logging.critical(f"Ocorreu um erro fatal no protótipo: {e}", exc_info=True)



