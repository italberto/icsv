"""
Exemplo 01 — Leitura básica
Demonstra como carregar um CSV, inspecionar e iterar sobre as linhas.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV

# Carregar — delimitador, cabeçalho e quebra de linha são detectados automaticamente
dados = ICSV(os.path.join(os.path.dirname(__file__), 'colaboradores.csv'))

# Informações gerais
print(dados.info())

# Prévia das primeiras 5 linhas
print("--- Prévia ---")
print(dados.preview())

# Número de linhas
print(f"Total de colaboradores: {len(dados)}")

# Acesso por índice
primeira = dados[0]
print(f"\nPrimeiro colaborador: {primeira.nome} ({primeira.cargo})")

# Iteração com acesso por nome de coluna
print("\n--- Colaboradores de São Paulo ---")
for linha in dados:
    if linha.cidade == "São Paulo":
        print(f"  {linha.nome} — {linha.departamento}")
