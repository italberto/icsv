"""
Exemplo 06 — Exportação e integração
Demonstra salvar_como, to_list_of_dicts, from_list_of_dicts e concatenar.
"""
import sys, os, json, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV

dados = ICSV(os.path.join(os.path.dirname(__file__), 'colaboradores.csv'))

# Salvar subconjunto filtrado em novo arquivo
ti = dados.filtrar_por_coluna('departamento', 'TI')
saida = os.path.join(os.path.dirname(__file__), 'ti.csv')
ti.salvar_como(saida)
print(f"Salvo: {saida} ({len(ti)} linhas)")

# Exportar para lista de dicionários (integração com requests, banco de dados, etc.)
lista = dados.selecionar_colunas(['nome', 'departamento', 'salario']).to_list_of_dicts()
print(f"\nto_list_of_dicts — primeiros 2 registros:")
for item in lista[:2]:
    print(f"  {item}")

# Criar ICSV a partir de lista de dicionários (ex.: resposta de uma API)
novos = [
    {"nome": "Paula Vaz",    "departamento": "TI",         "salario": "6100"},
    {"nome": "Rafael Lima",  "departamento": "Financeiro",  "salario": "5400"},
]
importados = ICSV.from_list_of_dicts(novos)
print(f"\nfrom_list_of_dicts: {len(importados)} linhas, colunas: {importados.cabecalho.campos}")

# Concatenar arquivos com a mesma estrutura
base = dados.selecionar_colunas(['nome', 'departamento', 'salario'])
combinado = base + importados
print(f"\nConcatenado: {len(combinado)} linhas")

# Exportar para JSON
info = json.loads(dados.info_json())
print(f"\ninfo_json:")
print(json.dumps(info, ensure_ascii=False, indent=2))

# Limpar arquivo gerado no exemplo
if os.path.exists(saida):
    os.remove(saida)
