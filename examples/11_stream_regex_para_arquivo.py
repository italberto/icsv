"""
Exemplo 11 — Stream + regex direto para arquivo
Demonstra leitura em stream e exportação incremental usando regex,
sem materializar o resultado inteiro em memória.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV


entrada = "/tmp/log_aplicacao.csv"
saida = "/tmp/log_aplicacao_erros_5xx.csv"

# Gera um arquivo de exemplo com status variados
with open(entrada, "w", encoding="utf-8", newline="") as f:
    f.write("timestamp,servico,rota,status,mensagem\n")
    for i in range(150000):
        if i % 2000 == 0:
            status = "500"
            mensagem = "Falha interna"
        elif i % 3500 == 0:
            status = "503"
            mensagem = "Serviço indisponível"
        elif i % 1700 == 0:
            status = "404"
            mensagem = "Não encontrado"
        else:
            status = "200"
            mensagem = "OK"

        f.write(
            f"2026-03-31T{i % 24:02d}:00:00,api-core,/v1/recurso/{i % 900},{status},{mensagem}\n"
        )

print(f"Arquivo de entrada: {entrada}")
print(f"Tamanho entrada: {os.path.getsize(entrada) / 1024 / 1024:.2f} MB")

# Lê em stream e exporta incrementalmente só erros 5xx
dados = ICSV(entrada, possui_cabecalho=True, modo_leitura="stream")
linhas_gravadas = dados.salvar_filtrado_por_regex(saida, "status", r"^5\d\d$")

print(f"Arquivo de saída: {saida}")
print(f"Linhas 5xx gravadas: {linhas_gravadas}")

# Prévia do resultado
resultado = ICSV(saida, possui_cabecalho=True, modo_leitura="stream")
print("\nPrévia do arquivo filtrado por regex:")
print(resultado.preview())

# Limpeza (descomente para remover os arquivos gerados)
# os.unlink(entrada)
# os.unlink(saida)
