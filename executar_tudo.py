# Script de Orquestração Principal para o Jenkins

# Ajustando importações para os nomes padronizados no DB
import E1_extracao_DB as E1
import E2_interesse_DB as E2
import E3_noticia_DB as E3
import E4_alvo_DB as E4
import E5_alerta_DB as E5
import E6_cvm_monitor_DB as CVM
import E7_ceres_monitor_DB as CERES
import logging
import sys
import time

# Configuração simples de logging para o orquestrador
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

logger.info("=============================================")
logger.info("===> INICIANDO O PIPELINE DE ORQUESTRAÇÃO <===")
logger.info("=============================================")

try:
    # --- FLUXO PRINCIPAL (SAURION) ---
    
    logger.info("\n--- 1/7: Executando E1: Extração e Ingestão de Links ---")
    E1.main()
    logger.info("--- E1 CONCLUÍDA. ---")

    logger.info("\n--- 2/7: Executando E2: Classificação de Interesse (LLM) ---")
    E2.main()
    logger.info("--- E2 CONCLUÍDA. ---")

    logger.info("\n--- 3/7: Executando E3: Extração do Texto Principal (Newspaper) ---")
    E3.main()
    logger.info("--- E3 CONCLUÍDA. ---")

    logger.info("\n--- 4/7: Executando E4: Classificação de Alvo (LLM) ---")
    E4.main()
    logger.info("--- E4 CONCLUÍDA. ---")

    logger.info("\n--- 5/7: Executando E5: Envio de Alertas (CD) ---")
    E5.main()
    logger.info("--- E5 CONCLUÍDA. ---")

    logger.info("\n==> FLUXO SAURION (E1-E5) FINALIZADO COM SUCESSO! <==")
    
    # --- FLUXOS INDEPENDENTES ---

    logger.info("\n--- 6/7: Executando E6: Monitoramento CVM (MUNIN) ---")
    CVM.main()
    logger.info("--- E6 CONCLUÍDA. ---")

    logger.info("\n--- 7/7: Executando E7: Monitoramento Ceres (HALL) ---")
    CERES.main()
    logger.info("--- E7 CONCLUÍDA. ---")
    
    logger.info("\n=============================================")
    logger.info("===> ORQUESTRAÇÃO COMPLETA: SUCESSO TOTAL! <===")
    logger.info("=============================================")
    
except Exception as e:
    logger.error(f"\nOcorreu um ERRO CRÍTICO em uma das etapas: {e}")
    logger.error("A execução foi interrompida.")
    sys.exit(1) # Código de saída 1 para indicar falha no Jenkins