import os
import time
import pandas as pd
import feedparser
from newspaper import Article, Config
import google.generativeai as genai
import json
from httplib2 import Http
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs
from typing import List, Dict, Any, Set, Optional, Tuple

# Imports para o Selenium aprimorados
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Import para a biblioteca de bypass
import cloudscraper

# Import para o sistema de logging
import logging

# ==================== CONFIGURAÇÕES GLOBAIS ====================
load_dotenv() # Carrega variáveis de um arquivo .env para desenvolvimento local

# Função para configurar o logging
def setup_logging():
    """Configura o logging para exibir mensagens formatadas no console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)

# Dicionário de Feeds RSS
RSS_FEEDS  = {
    'xp investimento': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/5822277793724032524',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/13683415329780998272',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/10269129280916203083',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/9550017878123396804',
        'https://news.google.com/rss/search?q=xp%20investimentos%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    'vinci': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/5394093770400447553',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/7598054495566054039',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/13734905907790337986',
        'https://news.google.com/rss/search?q=Vinci%20Compass%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    'tivio': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/15089636150786167689',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/14878059582149958709',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/10293027038187432395',
        'https://news.google.com/rss/search?q=tivio%20capita%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    'tarpon': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/11130384113973110931',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/13474059744439098265',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/7060813333020958445',
        'https://news.google.com/rss/search?q=tarpon%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    'bnp': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/15848728452539530082',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/14146215433246553046',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/14146215433246553144',
        'https://news.google.com/rss/search?q=bnp%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
    'oceana': [
        'https://www.google.com.br/alerts/feeds/09404460482838700245/4978498206918616829',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/15423088151033479411',
        'https://www.google.com.br/alerts/feeds/09404460482838700245/10556121544099713022',
        'https://news.google.com/rss/search?q=oceana%20investimentos%20when%3A1d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419'
    ],
}

NOME_PLANILHA = 'historico-noticias'
NOME_ABA = 'historico'

# ==================== CLASSE PRINCIPAL DO BOT ====================

class ComplianceNewsBot:
    """Um bot para monitorar, analisar e reportar notícias de compliance."""
    def __init__(self):
        """Inicializa todos os componentes do bot."""
        logging.info("Iniciando o Bot de Compliance...")
        self.driver = self._configurar_selenium()
        self.scraper = cloudscraper.create_scraper(browser={'custom': 'Mozilla/5.0'})
        self.config_newspaper = self._configurar_newspaper()
        self.model_gemini = self._configurar_modelo_ia()
        self.sheet_aba = self._conectar_planilha()
        self.chat_webhook_url = os.getenv("CHAT_WEBHOOK_URL_SAURON")

    def _configurar_selenium(self) -> webdriver.Chrome:
        """Configura e retorna uma instância do WebDriver do Selenium."""
        logging.info("Configurando o Selenium WebDriver com webdriver-manager...")
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver

    def close(self):
        """Fecha o driver do Selenium e libera os recursos."""
        if self.driver:
            logging.info("Fechando o Selenium WebDriver.")
            self.driver.quit()

    def _configurar_newspaper(self) -> Config:
        """Configura e retorna um objeto de configuração para a biblioteca Newspaper."""
        config = Config()
        config.browser_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        config.request_timeout = 20
        config.language = 'pt'
        return config

    def _configurar_modelo_ia(self) -> genai.GenerativeModel:
        """Configura e retorna o modelo generativo do Gemini."""
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            logging.error("Chave de API do Gemini não encontrada. Verifique as variáveis de ambiente.")
            raise ValueError("Chave de API do Gemini não encontrada. Verifique o arquivo .env ou as credenciais do Jenkins.")
        genai.configure(api_key=api_key)
        
        system_instruction = """
Você é um Analista Sênior de Risco Reputacional. Sua tarefa é analisar uma notícia e retornar uma classificação de risco multifatorial em formato JSON.

**Análise em 4 Passos:**

1.  **Foco na Gestora Alvo:** A análise deve se concentrar exclusivamente na GESTORA ALVO. Se a gestora for mencionada apenas de forma secundária (ex: como fonte de um comentário, em uma lista de cotações, em publicidade), o risco deve ser classificado como "Nenhum".

2.  **Fator de Risco:** Classifique o evento central da notícia em UMA das categorias: 

    {
  "Integridade e Ética": "Refere-se a eventos de fraude, corrupção, suborno, lavagem de dinheiro ou manipulação de mercado. Ações que questionam a honestidade e os princípios morais da gestora ou de seus funcionários.",
  "Fiduciário e de Produto": "Envolve falhas no dever de agir no melhor interesse do cliente. Inclui má gestão de fundos, performance ruim por negligência, taxas abusivas ou propaganda enganosa sobre produtos de investimento.",
  "Operacional e de Segurança": "Cobre falhas nos processos e sistemas internos. Exemplos incluem erros de negociação, falhas de compliance, violações de cibersegurança e vazamento de dados de clientes ou da empresa.",
  "Governança e Liderança": "Focado em escândalos, declarações controversas ou má conduta da alta gestão (C-level, diretoria). Inclui também conflitos de interesse e disputas de poder no conselho.",
  "Legal e Regulatório": "Relacionado a violações de leis ou de normas de órgãos reguladores (como a CVM). Inclui o início de investigações formais, processos judiciais, multas e outras sanções.",
  "ESG e Social": "Abrange controvérsias ligadas a fatores Ambientais, Sociais e de Governança. Exemplos: danos ao meio ambiente, práticas trabalhistas inadequadas, falta de diversidade ou envolvimento em polêmicas sociais.",
  "Risco de Terceiros": "Risco que se origina de um parceiro de negócios ou de uma empresa na qual a gestora tem um investimento significativo (risco de contágio). O dano reputacional vem por associação.",
  "Nenhum": "Categoria utilizada quando a notícia é positiva, neutra ou quando a gestora é mencionada apenas de forma secundária, sem ser o foco de qualquer evento adverso."
}
3.  **Impacto Inerente (1-5):** Avalie a gravidade potencial do evento para a GESTORA ALVO:
    *   `1`: Insignificante.
    *   `2`: Menor.
    *   `3`: Moderado.
    *   `4`: Grave.
    *   `5`: Catastrófico.

4.  **Modificador de Resposta (0.5, 1.0 ou 1.5):** Analise o texto e procure uma declaração ou resposta oficial da GESTORA ALVO sobre o evento.
    *   **Atribua `0.5` (Positiva):** Se a gestora apresentar uma resposta forte, com evidências, ou anunciar medidas corretivas imediatas e eficazes. (Ex: "...apresentou documentos que comprovam...", "...afastou os envolvidos e iniciou auditoria...").
    *   **Atribua `1.5` (Negativa):** Se a resposta for evasiva, contraditória com os fatos, ou se a gestora agravar a situação com declarações polêmicas. (Ex: "...negou, mas e-mails indicam o contrário...", "...minimizou o ocorrido...").
    *   **Atribua `1.0` (Neutra ou Ausente):** **Este é o padrão.** Use este valor se a notícia **não mencionar nenhuma resposta** da gestora, ou se a resposta for uma declaração padrão e não-comprometida. (Ex: "...a empresa não comentou...", "...disse que está apurando os fatos...").

**Formato da Resposta:**
Sua resposta deve ser **APENAS UM OBJETO JSON VÁLIDO**, com três chaves.

Exemplo 1 (Crise com boa resposta):
```json
{
  "fator_de_risco": "Integridade_e_Etica",
  "impacto_inerente": 5,
  "modificador_resposta": 0.5
}
Exemplo 2 (Notícia negativa sem menção de resposta da empresa):
{
  "fator_de_risco": "Legal_e_Regulatorio",
  "impacto_inerente": 4,
  "modificador_resposta": 1.0
}
"""
        return genai.GenerativeModel(model_name="gemini-2.5-flash", system_instruction=system_instruction)
    
    def _conectar_planilha(self) -> gspread.Worksheet:
        """Conecta-se à planilha do Google Sheets."""
        logging.info("Conectando à planilha do Google Sheets...")
        
        # Pega o nome do arquivo a partir da variável de ambiente
        credentials_filename = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
        if not credentials_filename:
            raise ValueError("Caminho para o arquivo de credenciais do Google não encontrado. Defina a variável de ambiente GOOGLE_APPLICATION_CREDENTIALS.")
            
        # --- INÍCIO DA MODIFICAÇÃO ---
        # Constrói o caminho absoluto para o arquivo de credenciais
        # Isso garante que o script sempre encontre o arquivo, não importa de onde seja executado.
        script_dir = os.path.dirname(os.path.abspath(__file__))
        credentials_path = os.path.join(script_dir, credentials_filename)

        # Adiciona uma verificação para garantir que o arquivo foi encontrado no caminho correto
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"ERRO CRÍTICO: O arquivo de credenciais '{credentials_filename}' não foi encontrado no diretório do script: {script_dir}")
        # --- FIM DA MODIFICAÇÃO ---
            
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        
        # Usa o caminho absoluto em vez do nome do arquivo
        creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        
        cliente = gspread.authorize(creds)
        planilha = cliente.open(NOME_PLANILHA)
        return planilha.worksheet(NOME_ABA)

    def _extrair_url_real(self, url_original: str) -> str:
        """Extrai a URL de destino de um link do Google Alerts ou Google News."""
        # A lógica do Selenium para resolver links do Google News está correta e permanece
        if 'news.google.com/rss/articles/' in url_original:
            try:
                url_inicial = self.driver.current_url
                self.driver.get(url_original)
                WebDriverWait(self.driver, 10).until(lambda driver: driver.current_url != url_inicial)
                url_final = self.driver.current_url
                logging.info(f"  -> Link Google News resolvido para: {url_final[:80]}...")
                return url_final
            except TimeoutException:
                logging.warning(f"Timeout ao resolver {url_original}. Retornando URL atual: {self.driver.current_url}")
                return self.driver.current_url
            except Exception as e:
                logging.error(f"Falha no Selenium ao resolver {url_original}: {e}")
                return url_original

        # A lógica para extrair de outros links do Google (como Alertas)
        try:
            parsed_url = urlparse(url_original)
            
            return parse_qs(parsed_url.query)['url'][0]

        except (KeyError, IndexError):
            # Se não for uma URL de alerta ou se não tiver o parâmetro 'url', retorna a original
            return url_original
    
    def _obter_html_com_selenium(self, url: str) -> Optional[str]:
        """Usa o Selenium para carregar a página e retornar seu código-fonte HTML."""
        try:
            logging.info(f"  -> Usando fallback (Selenium) para: {url[:80]}...")
            self.driver.get(url)
            return self.driver.page_source
        except Exception as e:
            logging.error(f"Falha total ao obter HTML com Selenium para {url}: {e}")
            return None

    def _processar_artigo_worker(self, entry_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Worker para baixar e extrair o conteúdo de um artigo."""
        link_limpo = entry_data['link_limpo']
        html_content = None
        article = Article(link_limpo, config=self.config_newspaper)

        try:
            logging.info(f"Processando com Cloudscraper: {link_limpo[:80]}...")
            response = self.scraper.get(link_limpo, timeout=15)
            response.raise_for_status()
            html_content = response.text
            article.download(input_html=html_content)
            article.parse()

            if len(article.text.strip()) < 150:
                logging.warning(f"Conteúdo insuficiente via Cloudscraper. Tentando Selenium...")
                article.text = ""

        except Exception as e:
            logging.warning(f"Falha com Cloudscraper para {link_limpo}: {e}. Acionando fallback...")
            article.text = ""

        if not article.text.strip():
            html_content = self._obter_html_com_selenium(link_limpo)
            if html_content:
                try:
                    article.download(input_html=html_content)
                    article.parse()
                except Exception as e:
                    logging.error(f"Falha ao processar HTML do Selenium para {link_limpo}: {e}")

        if not article.text or len(article.text.strip()) < 100:
            logging.error(f"Conteúdo final vazio ou insuficiente para: {link_limpo}")
            return None

        entry_data['texto'] = article.text
        entry_data['title'] = article.title or entry_data.get('title')
        entry_data['fonte'] = article.source_url or urlparse(link_limpo).netloc
        return entry_data

    def extrair_noticias(self) -> Tuple[pd.DataFrame, int]:
        """
        Extrai notícias dos feeds, resolve os links, baixa o conteúdo 
        e retorna um DataFrame com as notícias e a contagem de itens brutos encontrados.
        """
        logging.info("Buscando notícias nos feeds RSS...")
        
        tarefas_brutas = []
        for gestora, urls in RSS_FEEDS.items():
            for url in urls:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
                feed = feedparser.parse(url, request_headers=headers)
                
                for entry in feed.entries:
                    tarefas_brutas.append({
                        'gestora': gestora,
                        'title': entry.title,
                        'link_original': entry.link,
                        'date': entry.get('published', 'N/A'),
                    })
        
        total_encontradas_brutas = len(tarefas_brutas)

        if not tarefas_brutas:
            return pd.DataFrame(), total_encontradas_brutas
            
        logging.info(f"Encontradas {total_encontradas_brutas} notícias brutas. Resolvendo links...")

        tarefas_prontas = []
        for tarefa in tarefas_brutas:
            link_limpo = self._extrair_url_real(tarefa['link_original'])
            tarefa['link_limpo'] = link_limpo
            tarefas_prontas.append(tarefa)

        logging.info("Links resolvidos. Iniciando download paralelo do conteúdo...")

        noticias_processadas = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_entry = {executor.submit(self._processar_artigo_worker, tarefa): tarefa for tarefa in tarefas_prontas}
            for future in as_completed(future_to_entry):
                resultado = future.result()
                if resultado:
                    noticias_processadas.append(resultado)
        
        logging.info(f"Extração concluída. {len(noticias_processadas)} artigos com conteúdo obtido.")
        if not noticias_processadas:
            return pd.DataFrame(), total_encontradas_brutas
            
        df_final = pd.DataFrame(noticias_processadas).drop_duplicates(subset=['link_limpo'], keep='first')
        
        return df_final, total_encontradas_brutas

    def _classificar_risco(self, noticia: tuple) -> Dict[str, Any]:
        """Usa o Gemini para classificar risco, impacto e modificador de uma notícia."""
        prompt = f"GESTORA ALVO: {noticia.gestora}\n---\nNOTÍCIA PARA ANÁLISE:\n---\n{noticia.texto}"
        
        try:
            response = self.model_gemini.generate_content(
                prompt,
                generation_config={"temperature": 0.2, "response_mime_type": "application/json"}
            )
            tokens = response.usage_metadata.total_token_count if hasattr(response, 'usage_metadata') else 0
            
            cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
            data = json.loads(cleaned_response)

            risco = data.get('fator_de_risco', 'sem_categoria').lower()
            impacto = int(data.get('impacto_inerente', 0))
            modificador = float(data.get('modificador_resposta', 1.0))
            
            if modificador not in [0.5, 1.0, 1.5]:
                modificador = 1.0

            return {'risco': risco, 'impacto_inerente': impacto, 'modificador_resposta': modificador, 'tokens_utilizados': tokens}

        except (json.JSONDecodeError, AttributeError, ValueError) as e:
            logging.error(f"Falha na análise da IA para {noticia.link_limpo}: {e}")
            return {'risco': 'erro_analise_ia', 'impacto_inerente': 0, 'modificador_resposta': 1.0, 'tokens_utilizados': 0}
        except Exception as e:
            logging.error(f"Falha na API do Gemini para {noticia.link_limpo}: {e}")
            if "429" in str(e) or "rate limit" in str(e).lower():
                time.sleep(60)
            return {'risco': 'erro_api', 'impacto_inerente': 0, 'modificador_resposta': 1.0, 'tokens_utilizados': 0}

    def _carregar_historico_links(self) -> Set[str]:
        """Carrega a coluna de links da planilha para verificação rápida."""
        logging.info("Carregando histórico de links da planilha...")
        try:
            return set(self.sheet_aba.col_values(1)[1:])
        except Exception as e:
            logging.error(f"Não foi possível carregar o histórico: {e}")
            return set()

    def _get_risk_level(self, risco_final: float) -> str:
        """Classifica a pontuação de risco final em um nível textual."""
        if risco_final > 6:
            return "Crítico"
        elif 4 < risco_final <= 6:
            return "Alto"
        elif 2 < risco_final <= 4:
            return "Médio"
        elif 0.5 <= risco_final <= 2:
            return "Baixo"
        else:
            return "Insignificante"

    def _enviar_alerta_chat(self, gestora: str, titulo: str, link: str, risco_categoria: str, nivel_risco: str, risco_final: float, fonte: str):
        """Envia um alerta formatado para o Google Chat via Webhook."""
        if not self.chat_webhook_url:
            logging.warning("Webhook do Chat não configurado. Alerta não enviado.")
            return
        
        app_message = {
            "text": (
                f"🚨 *Alerta de Risco: {nivel_risco.upper()}* 🚨\n\n"
                f"A gestora *{gestora.upper()}* foi noticiada em evento de risco relevante!\n\n"
                f"∙ Categoria: *{risco_categoria.replace('_', ' ').upper()}*\n"
                f"∙ Nível de Risco: *{nivel_risco}*\n"
                f"∙ Pontuação Final: *{risco_final:.2f}*\n\n"
                f"*Notícia:* {titulo}\n"
                f"*Fonte*: <{link}|Clique para ler>"
            )
        }
        
        http_obj = Http()
        try:
            response, content = http_obj.request(
                uri=self.chat_webhook_url,
                method="POST",
                headers={"Content-Type": "application/json; charset=UTF-8"},
                body=json.dumps(app_message),
            )
            if response.status == 200:
                logging.info(f"  -> Alerta de risco '{nivel_risco}' enviado com sucesso para o Chat sobre '{gestora}'.")
            else:
                logging.error(f"Falha ao enviar alerta. Status: {response.status}, Resposta: {content.decode('utf-8')}")
        except Exception as e:
            logging.error(f"Exceção ao enviar alerta para o chat: {e}")

    def run(self):
        """Ponto de entrada principal para executar todo o fluxo do bot."""
        start_time = time.time()
        logging.info("-" * 50)
        
        historico_links = self._carregar_historico_links()
        
        df_noticias, total_encontradas = self.extrair_noticias()
        total_baixadas = len(df_noticias)

        if df_noticias.empty:
            logging.info("Nenhuma notícia foi encontrada ou baixada nos feeds.")
            end_time = time.time()
            logging.info("=" * 20 + " RESUMO DA EXECUÇÃO " + "=" * 20)
            logging.info(f"Total de notícias encontradas nos feeds: {total_encontradas}")
            logging.info(f"Total de notícias com conteúdo baixado: {total_baixadas}")
            logging.info(f"Total de notícias novas adicionadas à planilha: 0")
            logging.info(f"Processo concluído em {end_time - start_time:.2f} segundos.")
            logging.info("=" * 58)
            return

        df_novas = df_noticias[~df_noticias['link_limpo'].isin(historico_links)].copy()
        
        if df_novas.empty:
            logging.info("Nenhuma notícia nova para processar. Finalizando.")
            end_time = time.time()
            logging.info("=" * 20 + " RESUMO DA EXECUÇÃO " + "=" * 20)
            logging.info(f"Total de notícias encontradas nos feeds: {total_encontradas}")
            logging.info(f"Total de notícias com conteúdo baixado: {total_baixadas}")
            logging.info(f"Total de notícias novas adicionadas à planilha: 0")
            logging.info(f"Processo concluído em {end_time - start_time:.2f} segundos.")
            logging.info("=" * 58)
            return

        logging.info(f"Encontradas {len(df_novas)} novas notícias para análise.")
        
        resultados = []
        for noticia in df_novas.itertuples(index=False):
            logging.info(f"Analisando: {noticia.gestora.upper()} - {noticia.title}")
            
            resultado_analise = self._classificar_risco(noticia)
            risco = resultado_analise['risco']
            impacto_inerente = resultado_analise['impacto_inerente']
            modificador = resultado_analise['modificador_resposta']
            
            risco_final = impacto_inerente * modificador
            nivel_de_risco = self._get_risk_level(risco_final)
            
            resultados.append({
                'link_limpo': noticia.link_limpo, 
                'gestora': noticia.gestora, 
                'date': noticia.date,
                'title': noticia.title, 
                'fonte': noticia.fonte,
                'risco': risco,
                'impacto_inerente': impacto_inerente,
                'modificador_resposta': modificador,
                'risco_final': risco_final,
                'nivel_de_risco': nivel_de_risco,
                'tokens_utilizados': resultado_analise['tokens_utilizados'],
                'texto': noticia.texto
            })
            
            if nivel_de_risco in ['Médio', 'Alto', 'Crítico']:
                self._enviar_alerta_chat(
                    gestora=noticia.gestora, 
                    titulo=noticia.title, 
                    link=noticia.link_limpo, 
                    risco_categoria=risco,
                    nivel_risco=nivel_de_risco, 
                    risco_final=risco_final, 
                    fonte=noticia.fonte
                )
            
            time.sleep(3) 

        df_resultados = pd.DataFrame(resultados)
        total_adicionadas = len(df_resultados)
        
        if not df_resultados.empty:
            logging.info(f"Atualizando a planilha com {total_adicionadas} novos resultados.")
            
            colunas_planilha = [
                'link_limpo', 'gestora', 'date', 'title', 'fonte', 'risco', 
                'impacto_inerente', 'modificador_resposta', 'risco_final', 
                'nivel_de_risco', 'tokens_utilizados', 'texto'
            ]
            
            df_para_planilha = df_resultados.reindex(columns=colunas_planilha)

            df_para_planilha['risco_final'] = df_para_planilha['risco_final'].apply(lambda x: f"{x:.2f}")

            lista_para_inserir = df_para_planilha.astype(str).values.tolist()
            
            self.sheet_aba.append_rows(lista_para_inserir, value_input_option='USER_ENTERED')
            logging.info("Planilha atualizada com sucesso.")

        end_time = time.time()
        logging.info("=" * 20 + " RESUMO DA EXECUÇÃO " + "=" * 20)
        logging.info(f"Total de notícias encontradas nos feeds: {total_encontradas}")
        logging.info(f"Total de notícias com conteúdo baixado: {total_baixadas}")
        logging.info(f"Total de notícias novas adicionadas à planilha: {total_adicionadas}")
        logging.info(f"Processo concluído em {end_time - start_time:.2f} segundos.")
        logging.info("=" * 58)

# ==================== PONTO DE ENTRADA ====================
if __name__ == "__main__":
    setup_logging()
    bot = None
    try:
        bot = ComplianceNewsBot()
        bot.run()
    except Exception as e:
        logging.critical(f"Ocorreu um erro fatal na aplicação: {e}", exc_info=True)
        raise
    finally:
        if bot:
            bot.close()
