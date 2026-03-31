"""
Exemplo 04 — Análise de dados
Demonstra valores_unicos, contar_por, head/tail, slice e deduplicar.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV

dados = ICSV(os.path.join(os.path.dirname(__file__), 'colaboradores.csv'))

# Valores únicos
cidades = dados.valores_unicos('cidade')
print(f"Cidades: {sorted(cidades)}")

departamentos = dados.valores_unicos('departamento')
print(f"Departamentos: {sorted(departamentos)}")

# Contagem por valor
print("\n--- Colaboradores por departamento ---")
for dept, qtd in sorted(dados.contar_por('departamento').items()):
    print(f"  {dept}: {qtd}")

print("\n--- Colaboradores por status ---")
for status, qtd in dados.contar_por('status').items():
    print(f"  {status}: {qtd}")

# head e tail
print("\n--- Primeiros 3 ---")
for linha in dados.head(3):
    print(f"  {linha.nome}")

print("\n--- Últimos 3 ---")
for linha in dados.tail(3):
    print(f"  {linha.nome}")

# Slice — retorna novo ICSV
pagina = dados[5:10]
print(f"\nLinhas 5 a 9: {len(pagina)} registros")

# Selecionar colunas — projeção
resumo = dados.selecionar_colunas(['nome', 'departamento', 'salario'])
print(f"\nColunas do resumo: {resumo.cabecalho.campos}")
print(resumo.preview())

# Deduplicar por coluna
# Simula entradas duplicadas concatenando o arquivo consigo mesmo
com_duplicatas = dados + dados
print(f"\nCom duplicatas: {len(com_duplicatas)}")
sem_duplicatas = com_duplicatas.deduplicar('nome')
print(f"Após deduplicar por 'nome': {len(sem_duplicatas)}")
