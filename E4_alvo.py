from dotenv import load_dotenv
import os
import time
import json
from groq import Groq
import pandas as pd

# Seus 3 arquivos CSV
CSV_IN = r"C:\Users\Arthur Braz\monitoramento_midia\noticias_para_envio.csv"
CSV_OUT = r"C:\Users\Arthur Braz\monitoramento_midia\noticias_para_envio.csv"
CSV_HIST = r"C:\Users\Arthur Braz\monitoramento_midia\noticias_para_historico.csv"

MODEL = "gemma2-9b-it"

# --------- Utilidades de log ---------
def log(msg: str):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def sanitize_text(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def build_prompt(gestora, titulo, subtitulo, texto):
    texto_truncado = texto[:1000]
    sufixo = "..." if len(texto) > 1000 else ""
    return f"""
Sua tarefa é determinar se a 'Gestora-alvo' é o sujeito principal da notícia e se é de interesse que a diretoria de uma empresa gaste tempo lendo a notícia na íntegra.
Responda com 'S' ou 'N' a categoria alvo com base nas seguintes regras:
**Responda 'S' (Sim) se:** A notícia é sobre uma ação da gestora, um resultado dela, ou se ela sofreu uma ação (ex: uma multa, uma aquisição).
**Responda 'N' (Não) se:** A gestora ou um de seus funcionários é apenas citado para comentar sobre o mercado, uma outra empresa ou uma tendência geral.
  
Se a sua resposta for S faça uma descrição de no máximo 400 caracteres sobre o motivo da classificação.
Se a sua resposta for N, NÃO ESCREVA NADA.

Responda **somente** com um JSON válido, sem texto extra, no formato:
{{"alvo":"S|N","descricao":"texto aqui ou vazio"}}

Agora, classifique:
Gestora-alvo: {gestora}
Título: {titulo}
Subtítulo: {subtitulo}
Texto: {texto_truncado}{sufixo}
""".strip()

# --------- Histórico: agora salva todas as linhas ---------
def append_to_history(df: pd.DataFrame, hist_path: str):
    """Anexa TODAS as linhas ao CSV de histórico, criando-o se não existir."""
    if df is None or df.empty:
        log("Nenhuma linha para enviar ao histórico.")
        return

    cols = list(df.columns)
    file_exists = os.path.exists(hist_path)
    mode = "a" if file_exists else "w"
    header = not file_exists

    df.to_csv(
        hist_path,
        sep=";",
        index=False,
        encoding="utf-8-sig",
        mode=mode,
        header=header,
        columns=cols,
    )
    log(f"{len(df)} linha(s) adicionada(s) ao histórico: {hist_path}")

def main():
    log("Carregando variáveis de ambiente (.env)...")
    load_dotenv()
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY não encontrado no .env")

    log("Iniciando cliente do modelo...")
    client = Groq(api_key=api_key)
    log(f"Modelo '{MODEL}' inicializado com sucesso.")

    if not os.path.exists(CSV_IN):
        log(f"ERRO: Arquivo de análise não encontrado: {CSV_IN}")
        return

    log(f"Lendo CSV de análise: {CSV_IN}")
    try:
        df = pd.read_csv(CSV_IN, sep=";", encoding="utf-8-sig")
    except Exception as e:
        log(f"ERRO ao ler CSV: {e}")
        return
    
    if df.empty:
        log("AVISO: CSV de análise está vazio. Nada para processar.")
        return
    
    required_columns = ["gestora", "titulo", "subtitulo", "texto"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        log(f"ERRO: O CSV precisa conter as colunas: {missing_columns}")
        return
    
    total = len(df)
    log(f"{total} notícias carregadas.")

    resultados_alvo = []
    resultados_desc = []
    respostas_brutas = []

    log("Classificando notícias...")
    for idx, row in df.iterrows():
        gestora = sanitize_text(row.get("gestora", ""))
        titulo = sanitize_text(row.get("titulo", ""))
        subtitulo = sanitize_text(row.get("subtitulo", ""))
        texto = sanitize_text(row.get("texto", ""))

        resumo_titulo = (titulo[:77] + "…") if len(titulo) > 80 else titulo
        log(f"[{idx+1}/{total}] Classificando notícia: \"{resumo_titulo}\"")

        system_msg = {
            "role": "system",
            "content": "Você é um classificador de notícias. Responda APENAS com JSON válido."
        }
        user_msg = {
            "role": "user",
            "content": build_prompt(gestora, titulo, subtitulo, texto)
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
                alvo = str(data.get("alvo", "")).upper()
                descricao = str(data.get("descricao", ""))

                if alvo not in {"S", "N"}:
                    log(f"[{idx+1}/{total}] Aviso: JSON válido, mas valor 'alvo' fora do padrão: {alvo}")
                    alvo = "N"
            except json.JSONDecodeError:
                log(f"[{idx+1}/{total}] ERRO: Resposta não é JSON válido. Resposta bruta: {text}")
                alvo, descricao = "N", ""

            resultados_alvo.append(alvo)
            resultados_desc.append(descricao)
            log(f"[{idx+1}/{total}] Notícia classificada -> alvo={alvo} | descricao={(descricao[:50] or 'N/A')}...")

        except Exception as e:
            msg = f"ERRO ao chamar a API: {e}"
            log(f"[{idx+1}/{total}] {msg}")
            respostas_brutas.append(f"ERRO: {e}")
            resultados_alvo.append("N")
            resultados_desc.append("")
        time.sleep(0.15)

    log("Salvando resultados no CSV de envio...")
    df["alvo"] = resultados_alvo
    df["descricao"] = resultados_desc
    df["justificativa_alvo"] = respostas_brutas

    try:
        df.to_csv(CSV_OUT, sep=";", index=False, encoding="utf-8-sig")
        log(f"Classificação concluída. Arquivo salvo em: {CSV_OUT}")
    except Exception as e:
        log(f"ERRO ao salvar CSV de envio: {e}")
        return

    # --- Anexa todas as linhas ao histórico ---
    try:
        append_to_history(df, CSV_HIST)
    except Exception as e:
        log(f"ERRO ao salvar histórico: {e}")

    classificados_s = int((df["alvo"] == "S").sum())
    classificados_n = int((df["alvo"] == "N").sum())
    log(f"Processamento finalizado: {classificados_s} notícias marcadas como 'S', {classificados_n} como 'N'")

if __name__ == "__main__":
    main()
