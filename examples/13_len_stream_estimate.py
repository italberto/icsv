"""
Exemplo 13 — len_stream_estimate em modo stream
Compara estimativa rápida de linhas com contagem exata em stream,
demonstrando ajuste fino de intervalo com parâmetro confianca.
"""
# ⚠️ LIMITAÇÃO — Campos Multilinhas:
#    len_stream_estimate() assume que cada \n marca fim de linha.
#    Se o CSV tiver campos com quebras de linha internas (entre aspas),
#    a estimativa ficará imprecisa. Isso raramente afeta logs de máquina,
#    mas é muito comum em arquivos gerados por formulários web (Moodle, etc).
#    Para estes casos, use len(dados) (varredura completa) ou stream mode.
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV


arquivo = "/tmp/log_len_estimate.csv"

# Gera arquivo sintético
with open(arquivo, "w", encoding="utf-8", newline="") as f:
    f.write("timestamp,usuario,status\n")
    for i in range(220000):
        status = "200" if i % 10 else "500"
        f.write(f"2026-03-31T{i % 24:02d}:00:00,user{i % 8000},{status}\n")

print(f"Arquivo: {arquivo}")
print(f"Tamanho: {os.path.getsize(arquivo) / 1024 / 1024:.2f} MB\n")

# Stream
dados = ICSV(arquivo, possui_cabecalho=True, modo_leitura="stream")

inicio = time.perf_counter()
estimado, intervalo = dados.len_stream_estimate(amostra_linhas=1500, retornar_intervalo=True)
tempo_estimado = time.perf_counter() - inicio

print("=== Estimativas com diferentes níveis de confiança ===")
print(f"(Padrão confianca=0.95)")
print(f"Estimativa: {estimado} linhas em {tempo_estimado:.4f}s")
print(f"Intervalo (95%): {intervalo[0]} .. {intervalo[1]}\n")

# Comparar com outros níveis de confiança
niveis = [0.80, 0.90, 0.99]
for conf in niveis:
    est, (inf, sup) = dados.len_stream_estimate(
        amostra_linhas=1500, retornar_intervalo=True, confianca=conf
    )
    largura = sup - inf
    print(f"Intervalo ({int(conf*100)}%): {inf} .. {sup} (largura: ±{largura//2})")

print()

# Contagem exata para validação
inicio = time.perf_counter()
exato = len(dados)  # em stream, varre todo o arquivo
tempo_exato = time.perf_counter() - inicio

diferenca = exato - estimado
erro_percentual = (abs(diferenca) / exato * 100) if exato else 0.0

print("=== Validação ===")
print(f"Contagem exata (stream): {exato} linhas em {tempo_exato:.4f}s")
print(f"Diferença: {diferenca} linhas ({erro_percentual:.4f}%)")
print(f"Ganho de tempo: {tempo_exato - tempo_estimado:.4f}s")
# os.unlink(arquivo)
