from newspaper import Article

urls_problematicas = [
    'https://br.investing.com/news/company-news/hays-plc-nomeia-bnp-paribas-como-corretor-corporativo-conjunto-93CH-1606497',
    'https://fusoesaquisicoes.com/acontece-no-setor/quem-comprou-a-fatia-do-xp-malls-e-da-syn-no-shopping-d/',
    'https://www.msn.com/pt-br/dinheiro/economia-e-negocios/tarifas-dos-eua-podem-cortar-crescimento-do-pib-em-0-5-ponto-diz-xp/ar-AA1Imr5m',
    'https://riconnect.rico.com.vc/analises/tarifas-de-50-dos-eua-ao-brasil-impactos-na-economia-e-na-bolsa/'
]

# lib para burlar o cloudflare
import cloudscraper


# v3 support is enabled by default
scraper = cloudscraper.create_scraper()
response = scraper.get(urls_problematicas[3])


html_bruto = response.text

artigo = Article(url='http://meu-artigo-local.com')
artigo.download(html_bruto)
artigo.parse()
print(artigo.text)


from selenium import webdriver
from newspaper import Article

urls_problematicas = [
    'https://br.investing.com/news/company-news/hays-plc-nomeia-bnp-paribas-como-corretor-corporativo-conjunto-93CH-1606497',
    'https://fusoesaquisicoes.com/acontece-no-setor/quem-comprou-a-fatia-do-xp-malls-e-da-syn-no-shopping-d/',
    'https://www.msn.com/pt-br/dinheiro/economia-e-negocios/tarifas-dos-eua-podem-cortar-crescimento-do-pib-em-0-5-ponto-diz-xp/ar-AA1Imr5m',
    'https://riconnect.rico.com.vc/analises/tarifas-de-50-dos-eua-ao-brasil-impactos-na-economia-e-na-bolsa/'
]


driver = webdriver.Chrome()

driver.get(urls_problematicas[3])

html = driver.page_source
 
driver.quit()

artigo = Article(url='http://meu-artigo-local.com')
artigo.download(html)
artigo.parse()
print(artigo.text)