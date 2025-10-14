from dotenv import load_dotenv
import os
import time
import json
from groq import Groq
import pandas as pd

CSV_OUT = r"C:\Users\Arthur Braz\monitoramento_midia\noticias_para_analise.csv"
CSV_IN =  r"C:\Users\Arthur Braz\monitoramento_midia\noticias_para_analise.csv"
CSV_HIST = r"C:\Users\Arthur Braz\monitoramento_midia\noticias_para_historico.csv"  

MODEL = "openai/gpt-oss-20b"

# --------- Utilidades de log ---------
def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def sanitize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def build_prompt(titulo, subtitulo):
    return f"""
Sua função é classificar se o conteúdo do título e subtítulo é do meu interesse e rotular com L0-L5.

Temas de interesse (exemplos de rótulo):
- Ações de órgãos reguladores como CVM ou Banco Central contra gestoras de investimentos. (L5)
- Fusões, aquisições ou mudanças significativas na estrutura societária de gestoras de investimentos. (L3)
- Problemas de compliance, fraudes, investigações ou processos judiciais contra gestoras de investimentos. (L4)
- Instabilidades tecnológicas ou problemas operacionais de gestoras de investimentos. (L1)
- Mudança no c-level e demissões em massa em gestoras de investimentos. (L2)

Critérios de irrelevância (classifique como L0 e interesse=N se QUALQUER um for verdadeiro):
- A notícia é apenas um resumo/boletim/lista de várias empresas sem um evento principal claro.
- A notícia é sobre marketing, análise de mercado genérica ou lançamento de produtos.

Exemplos:
- Título: CVM abre processo administrativo contra XYZ Gestora por supostas irregularidades
  Subtítulo: Regulador investiga possíveis infrações e falhas de compliance na Info Asset.
  Interesse: "S", Classificacao: "L4"

- Título: Gestora lança novo cartão black
  Subtítulo: Veja como obter o seu.
  Interesse: "N", Classificacao: "L0"

- Título: Falha de segurança paralisa serviços Invest Asset por horas
  Subtítulo: Ataque cibernético comprometeu operações e dados sensíveis.
  Interesse: "S", Classificacao: "L1"

- Título: Notícia da Vale, XP, Lupatech, Bemobi, Petrobras e outros destaques corporativos
  Subtítulo: Boletim diário com as principais notícias do mercado.
  Interesse: "N", Classificacao: "L0"

Responda **somente** com um JSON válido, sem texto extra, no formato:
{{"interesse":"S|N","classificacao":"L0|L1|L2|L3|L4|L5"}}

Agora, classifique:
Título: {titulo}
Subtítulo: {subtitulo}
""".strip()

def append_to_history(df: pd.DataFrame, hist_path: str):
    """Filtra interesse == 'N' e anexa no CSV de histórico, criando-o se não existir."""
    if "interesse" not in df.columns:
        log("Aviso: coluna 'interesse' não existe no DataFrame; nada a enviar ao histórico.")
        return

    to_append = df[df["interesse"].astype(str).str.upper() == "N"]
    if to_append.empty:
        log("Nenhuma linha com interesse == 'N' para enviar ao histórico.")
        return

    # Garante mesma ordem de colunas no histórico (usa as do df atual)
    cols = list(df.columns)

    # Se o arquivo não existe, grava com cabeçalho; senão, faz append sem cabeçalho
    file_exists = os.path.exists(hist_path)
    mode = "a" if file_exists else "w"
    header = not file_exists

    to_append.to_csv(
        hist_path,
        sep=";",
        index=False,
        encoding="utf-8-sig",
        mode=mode,
        header=header,
        columns=cols,
    )
    log(f"{len(to_append)} linha(s) adicionada(s) ao histórico: {hist_path}")

def main():
    log("Carregando variáveis de ambiente (.env)...")
    load_dotenv()
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY não encontrado no .env")

    log("Iniciando cliente do modelo...")
    client = Groq(api_key=api_key)
    log(f"Modelo '{MODEL}' inicializado com sucesso.")

    log(f"Lendo CSV de entrada: {CSV_IN}")
    df = pd.read_csv(CSV_IN, sep=";")
    if "titulo" not in df.columns or "subtitulo" not in df.columns:
        raise ValueError("O CSV precisa conter as colunas 'titulo' e 'subtitulo'.")
    total = len(df)
    log(f"{total} notícias carregadas.")

    resultados_interesse = []
    resultados_class = []
    respostas_brutas = []

    log("Classificando notícias...")
    for idx, row in df.iterrows():
        titulo = sanitize_text(row.get("titulo", ""))
        subtitulo = sanitize_text(row.get("subtitulo", ""))

        resumo_titulo = (titulo[:77] + "…") if len(titulo) > 80 else titulo
        log(f"[{idx+1}/{total}] Classificando notícia: \"{resumo_titulo}\"")

        system_msg = {
            "role": "system",
            "content": "Você é um classificador de notícias. Responda APENAS com JSON válido."
        }
        user_msg = {
            "role": "user",
            "content": build_prompt(titulo, subtitulo)
        }

        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[system_msg, user_msg],
                temperature=0.0,
            )
            text = resp.choices[0].message.content.strip()
            respostas_brutas.append(text)

            try:
                data = json.loads(text)
                interesse = str(data.get("interesse", "")).upper()
                classificacao = str(data.get("classificacao", "")).upper()
                if interesse not in {"S", "N"} or not classificacao.startswith("L"):
                    log(f"[{idx+1}/{total}] Aviso: JSON válido, mas fora do padrão esperado: {text}")
            except json.JSONDecodeError:
                log(f"[{idx+1}/{total}] ERRO: Resposta não é JSON válido. Resposta bruta: {text}")
                interesse, classificacao = "", ""

            resultados_interesse.append(interesse)
            resultados_class.append(classificacao)
            log(f"[{idx+1}/{total}] Notícia classificada -> interesse={interesse or '?'} | classificacao={classificacao or '?'}")

        except Exception as e:
            msg = f"ERRO ao chamar a API: {e}"
            log(f"[{idx+1}/{total}] {msg}")
            respostas_brutas.append(f"ERRO: {e}")
            resultados_interesse.append("")
            resultados_class.append("")

        time.sleep(0.15) 

    log("Salvando resultados no CSV de saída...")
    df["interesse"] = resultados_interesse
    df["classificacao"] = resultados_class
    df["resposta_modelo"] = respostas_brutas

    df.to_csv(CSV_OUT, sep=";", index=False, encoding="utf-8-sig")
    log(f"Classificação concluída. Arquivo salvo em: {CSV_OUT}")

    log("Atualizando arquivo de histórico com linhas de interesse == 'N'...")
    append_to_history(df, CSV_HIST)

if __name__ == "__main__":
    main()
