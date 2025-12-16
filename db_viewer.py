import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# --- Configuração de Ambiente e Caminhos ---
load_dotenv()

# Caminho do diretório 'data' (ajustado para o ambiente CI/CD)
DB_DIR = os.environ.get("DATA_DIR", "./data") 

# URLs dos Bancos de Dados
DB_URL_MAIN = os.getenv("DB_URL", f"sqlite:///{DB_DIR}/noticias_pipeline.db")
DB_URL_CVM = f"sqlite:///{DB_DIR}/cvm_sent.db"
DB_URL_CERES = f"sqlite:///{DB_DIR}/sent_links.db"

# Nomes das Tabelas
TABLE_MAIN = "noticias"
TABLE_CVM = "sent_notifications"
TABLE_CERES = "sent_links"

def get_db_engine(db_url: str):
    """Cria a engine do SQLAlchemy para o DB especificado."""
    try:
        engine = create_engine(db_url)
        # Testar a conexão
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return engine
    except SQLAlchemyError as e:
        print(f"🚨 ERRO: Não foi possível conectar ao DB em {db_url.split('///')[-1]}. Erro: {e}")
        return None

def view_table(engine, table_name: str):
    """Lê e exibe o conteúdo de uma tabela específica."""
    if engine is None:
        return
        
    print(f"\n=============================================")
    print(f"👁️ TABELA: {table_name} ({engine.url.database})")
    print(f"=============================================")
    
    try:
        # Lê a tabela inteira
        df = pd.read_sql_table(table_name, engine)
        
        if df.empty:
            print("Status: VAZIA.")
            return

        # Limpa o campo 'texto' para visualização no console (mostra apenas os primeiros 50 caracteres)
        if 'texto' in df.columns:
            df['texto'] = df['texto'].str.slice(0, 50) + (df['texto'].apply(lambda x: '...' if isinstance(x, str) and len(x) > 50 else ''))
        
        # Exibe o DataFrame formatado
        print(df.to_markdown(index=False))
        print(f"\nTotal de linhas: {len(df)}")
        
    except ValueError:
        print(f"Status: Tabela '{table_name}' não encontrada ou DB está vazio.")
    except Exception as e:
        print(f"Erro ao ler a tabela {table_name}: {e}")

def main():
    print("🚀 INICIANDO VISUALIZADOR DE BANCOS DE DADOS 🚀")

    # 1. Visualizar o DB Principal (noticias_pipeline.db)
    engine_main = get_db_engine(DB_URL_MAIN)
    if engine_main:
        view_table(engine_main, TABLE_MAIN)

    # 2. Visualizar o DB Secundário da CVM (cvm_sent.db)
    engine_cvm = get_db_engine(DB_URL_CVM)
    if engine_cvm:
        view_table(engine_cvm, TABLE_CVM)

    # 3. Visualizar o DB Secundário Ceres (sent_links.db)
    engine_ceres = get_db_engine(DB_URL_CERES)
    if engine_ceres:
        view_table(engine_ceres, TABLE_CERES)
        
    print("\n🏁 VISUALIZAÇÃO CONCLUÍDA. 🏁")

if __name__ == "__main__":
    main()