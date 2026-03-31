"""
Exemplo 03 — Manipulação de colunas e linhas
Demonstra como adicionar, remover e modificar colunas e linhas.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV, Linha

dados = ICSV(os.path.join(os.path.dirname(__file__), 'colaboradores.csv'))

# Adicionar coluna com valor padrão
dados.adicionar_coluna('bonus', '0')
print(f"Colunas após adicionar 'bonus': {dados.cabecalho.campos}")

# Modificar valores — salário com reajuste de 10% para TI
def reajuste(valor):
    return str(int(valor) + int(valor) * 10 // 100)

ti = dados.filtrar_por_coluna('departamento', 'TI')
ti.modificar_valores('salario', reajuste)

print("\n--- Salários TI após reajuste de 10% ---")
for linha in ti:
    print(f"  {linha.nome}: R$ {linha.salario}")

# Modificar uma coluna usando outra como parâmetro
# Aqui calculamos o bônus como 5% do salário
dados.modificar_valores('bonus', lambda s: str(int(s) * 5 // 100), coluna_parametro='salario')

# Renomear coluna
dados.atualizar_nome_coluna('status', 'situacao')
print(f"\nColunas após renomear: {dados.cabecalho.campos}")

# Remover coluna
dados.remover_coluna('email')
print(f"Colunas após remover 'email': {dados.cabecalho.campos}")

# Adicionar linha
nova = Linha(['Zara Melo', 'Fortaleza', 'TI', 'Desenvolvedora', '6000', 'ativo', '300'])
dados.adicionar_linha(nova)
print(f"\nTotal após adicionar linha: {len(dados)}")

# Remover linha por índice
dados.remover_linha(len(dados) - 1)
print(f"Total após remover linha: {len(dados)}")
