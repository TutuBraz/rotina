import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Column, String, DateTime, Boolean, UniqueConstraint, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# 1. Configurações de Ambiente
load_dotenv()
DB_URL = os.getenv("DB_URL", "sqlite:///./data/noticias_pipeline.db")
TABLE_NAME = "noticias"

def setup_database():
    """
    Cria a engine do DB e define/cria a tabela 'noticias' com o schema correto.
    """
    print(f"Iniciando setup do banco de dados em: {DB_URL}")
    try:
        engine = create_engine(DB_URL)
        metadata = MetaData()
        
        # Definição do Schema da Tabela 'noticias'
        noticias_table = Table(
            TABLE_NAME, 
            metadata,
            
            # COLUNAS ESSENCIAIS DA E1:
            Column('gestora', String(50), nullable=True),
            Column('titulo', String, nullable=True),
            Column('subtitulo', String, nullable=True),
            
            # CHAVE PRIMÁRIA/ÚNICA: Crucial para evitar duplicidade
            Column('url', String, primary_key=True), 
            
            # COLUNAS PARA PREENCHIMENTO POSTERIOR (E2, E3, etc.):
            Column('alvo', String(50), nullable=True),
            Column('classificacao', String(5), nullable=True), 
            Column('interesse', String(1), nullable=True), 
            Column('resposta_modelo', String, nullable=True),
            Column('texto', String, nullable=True),
            Column('descricao', String, nullable=True),
            Column('justificativa_alvo', String, nullable=True),
            
            # COLUNAS DE STATUS E RASTREAMENTO:
            Column('status_e2', String(20), default='PENDENTE'),
            Column('status_e3', String(20), default='PENDENTE'),
            Column('status_e4', String(20), default='PENDENTE'), 
            Column('status_e5', String(20), default='PENDENTE'), 
            Column('msg_e5_erro', String, nullable=True),
            Column('timestamp_e1', DateTime, default=datetime.now()),
            
            # Adiciona a restrição de unicidade na URL 
            UniqueConstraint('url', name='uix_url')
        )
        
        metadata.create_all(engine)
        
        print(f"✅ Setup concluído. Tabela '{TABLE_NAME}' criada/verificada com sucesso.")
        print("💡 Lembre-se de montar o volume no Jenkins para persistir o arquivo DB.")

    except SQLAlchemyError as e:
        print(f"🚨 ERRO ao configurar o banco de dados. Verifique a DB_URL e as permissões. Erro: {e}")
        return

if __name__ == "__main__":
    # Garante que a pasta 'data' existe para o SQLite (se você usar o fallback)
    os.makedirs(os.path.dirname(DB_URL.replace("sqlite:///", "")), exist_ok=True)
    setup_database()