"""
Exemplo 08 — Linhas irregulares
Demonstra como tratar CSV com linhas menores/maiores que o cabeçalho.
"""
import os
import sys
import warnings

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV


csv_corrompido = """nome,idade,cidade
Ana,30
Bruno,40,RJ,EXTRA
"""

print("=== Modo padrão: avisar ===")
with warnings.catch_warnings(record=True) as avisos:
    warnings.simplefilter("always")
    dados_avisar = ICSV(texto=csv_corrompido, possui_cabecalho=True)

for aviso in avisos:
    print(f"[aviso] {aviso.message}")

print("Linhas lidas (mantidas como vieram):")
for linha in dados_avisar:
    print(linha.campos)

print("\n=== Modo normalizado: preencher ===")
dados_preencher = ICSV(
    texto=csv_corrompido,
    possui_cabecalho=True,
    tratamento_linhas_irregulares="preencher",
)

print("Linhas lidas (normalizadas no parse):")
for linha in dados_preencher:
    print(linha.campos)

print("\nAcesso seguro por nome de coluna após normalização:")
print(f"Ana -> cidade: '{dados_preencher[0].cidade}'")
