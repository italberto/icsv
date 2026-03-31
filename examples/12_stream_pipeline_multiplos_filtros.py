"""
Exemplo 12 — Pipeline stream com múltiplos filtros (ETL leve)
Demonstra como combinar filtros em modo incremental sem materializar
um grande subconjunto na memória.
"""
import csv
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV


entrada = "/tmp/log_pipeline.csv"
saida = "/tmp/log_pipeline_filtrado.csv"

# Gera dataset sintético (simulando dump grande)
with open(entrada, "w", encoding="utf-8", newline="") as f:
    f.write("timestamp,usuario,curso,status,mensagem\n")
    for i in range(180000):
        usuario = f"user{i % 5000}"
        curso = f"course-{i % 200}"

        if i % 2200 == 0:
            status = "500"
            mensagem = "database timeout"
        elif i % 1700 == 0:
            status = "503"
            mensagem = "service unavailable"
        elif i % 1400 == 0:
            status = "500"
            mensagem = "disk error"
        elif i % 1000 == 0:
            status = "404"
            mensagem = "resource not found"
        else:
            status = "200"
            mensagem = "ok"

        f.write(
            f"2026-03-31T{i % 24:02d}:00:00,{usuario},{curso},{status},{mensagem}\n"
        )

print(f"Entrada: {entrada}")
print(f"Tamanho: {os.path.getsize(entrada) / 1024 / 1024:.2f} MB")

# Lê em stream
dados = ICSV(entrada, possui_cabecalho=True, modo_leitura="stream")

# Pipeline incremental:
# 1) status 5xx
# 2) mensagem contendo timeout|unavailable
regex_mensagem = re.compile(r"timeout|unavailable", re.IGNORECASE)
iter_5xx = dados.iter_filtrar_por_regex("status", r"^5\d\d$")
iter_final = (linha for linha in iter_5xx if regex_mensagem.search(linha.mensagem))

# Exporta incrementalmente sem materializar o resultado final
linhas_gravadas = 0
with open(saida, "w", encoding="utf-8", newline="") as file:
    escritor = csv.writer(file, delimiter=dados.delimitador, lineterminator=dados.quebra_linha)
    if dados.possui_cabecalho and dados.cabecalho:
        escritor.writerow(dados.cabecalho.campos)

    for linha in iter_final:
        escritor.writerow(linha.campos)
        linhas_gravadas += 1

print(f"Saída: {saida}")
print(f"Linhas gravadas no pipeline: {linhas_gravadas}")

# Prévia rápida
resultado = ICSV(saida, possui_cabecalho=True, modo_leitura="stream")
print("\nPrévia do resultado final:")
print(resultado.preview())

# Limpeza (descomente para remover os arquivos gerados)
# os.unlink(entrada)
# os.unlink(saida)
