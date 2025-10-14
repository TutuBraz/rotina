import E1_extracao as E1
import E2_interesse as E2
import E3_noticia as E3
import E4_alvo as E4
import E5_envio as E5

print("==> Iniciando a execução dos scripts... <==")

try:
    print("\n--- Executando Extração ---")
    E1.main()

    print("\n--- Executando Interesse ---")
    E2.main()

    print("\n--- Executando Notícia ---")
    E3.main()

    print("\n--- Executando Alvo ---")
    E4.main()

    print("\n--- Executando Envio ---")
    E5.main()

    print("\n==> Processo finalizado com sucesso! <==")

except Exception as e:
    print(f"\nOcorreu um erro: {e}")
    print("A execução foi interrompida.")
