"""
Exemplo 02 — Filtros e ordenação
Demonstra filtrar_por_coluna com padrões estilo SQL LIKE e ordenação.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV

dados = ICSV(os.path.join(os.path.dirname(__file__), 'colaboradores.csv'))

# Correspondência exata
ativos = dados.filtrar_por_coluna('status', 'ativo')
print(f"Ativos: {len(ativos)}")

# Contém (padrão %texto%)
analistas = dados.filtrar_por_coluna('cargo', '%Analista%')
print(f"Analistas: {len(analistas)}")

# Começa com (padrão texto%)
nomes_com_a = dados.filtrar_por_coluna('nome', 'A%')
print(f"Nomes que começam com A: {len(nomes_com_a)}")

# Termina com (padrão %texto)
emails_empresa = dados.filtrar_por_coluna('email', '%empresa.com')
print(f"E-mails @empresa.com: {len(emails_empresa)}")

# Encadeamento de filtros
ti_ativos = dados.filtrar_por_coluna('departamento', 'TI').filtrar_por_coluna('status', 'ativo')
print(f"\nTI ativos: {len(ti_ativos)}")
for linha in ti_ativos:
    print(f"  {linha.nome} — {linha.cargo}")

# Ordenação por salário (decrescente)
print("\n--- Top 5 maiores salários ---")
dados.order_by_field_name('salario', reverse=True, cast_type=int)
for linha in dados.head(5):
    print(f"  {linha.nome}: R$ {linha.salario}")
