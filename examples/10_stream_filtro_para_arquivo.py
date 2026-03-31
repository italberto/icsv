"""
Exemplo 10 — Stream + filtro direto para arquivo
Demonstra leitura em stream e exportação incremental sem materializar
um subconjunto grande na memória.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV


entrada = "/tmp/log_moodle.csv"
saida = "/tmp/log_moodle_erros_500.csv"

# Gera um arquivo de exemplo (simulando dump grande)
with open(entrada, "w", encoding="utf-8", newline="") as f:
    f.write("timestamp,usuario,recurso,status,tempo_ms\n")
    for i in range(120000):
        status = "500" if i % 997 == 0 else "200"
        f.write(f"2026-03-31T{i % 24:02d}:00:00,user{i % 1500},/course/{i % 800},{status},{50 + (i % 900)}\n")

print(f"Arquivo de entrada: {entrada}")
print(f"Tamanho entrada: {os.path.getsize(entrada) / 1024 / 1024:.2f} MB")

# Abre em stream: não carrega o arquivo inteiro em RAM
dados = ICSV(entrada, possui_cabecalho=True, modo_leitura="stream")

# Filtra e salva incrementalmente (linha a linha)
linhas_gravadas = dados.salvar_filtrado_por_coluna(saida, "status", "500")

print(f"Arquivo de saída: {saida}")
print(f"Linhas com status=500 gravadas: {linhas_gravadas}")

# Pré-visualização do resultado
resultado = ICSV(saida, possui_cabecalho=True, modo_leitura="stream")
print("\nPrévia do arquivo filtrado:")
print(resultado.preview())

# Limpeza (descomente se quiser manter os arquivos para inspeção)
# os.unlink(entrada)
# os.unlink(saida)
