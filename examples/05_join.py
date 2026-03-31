"""
Exemplo 05 — JOIN entre arquivos
Demonstra inner join e left join entre dois CSVs.

⚠️ IMPORTANTE — Limitação de Memória:
   O método join() carrega o arquivo da direita (outro_csv) inteiramente em memória.
   Para arquivos muito grandes, coloque sempre o arquivo MENOR no lado direito.
   
   Exemplo de melhor prática:
       # Coloque o arquivo menor à direita
       resultado = arquivo_grande.join(arquivo_pequeno, 'id', 'id')
       
       # Não faça assim (arquivo grande à direita):
       # resultado = arquivo_pequeno.join(arquivo_grande, 'id', 'id')  ← risco de MemoryError
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV

colaboradores = ICSV(os.path.join(os.path.dirname(__file__), 'colaboradores.csv'))
departamentos = ICSV(os.path.join(os.path.dirname(__file__), 'departamentos.csv'))

print("--- Colaboradores ---")
print(f"Colunas: {colaboradores.cabecalho.campos}")
print(f"Linhas:  {len(colaboradores)}\n")

print("--- Departamentos ---")
print(f"Colunas: {departamentos.cabecalho.campos}")
print(f"Linhas:  {len(departamentos)}\n")

# Inner join — apenas correspondências
resultado = colaboradores.join(departamentos, chave_esq='departamento', chave_dir='departamento')
print(f"--- INNER JOIN ---")
print(f"Colunas: {resultado.cabecalho.campos}")
print(f"Linhas:  {len(resultado)}\n")

for linha in resultado.head(5):
    print(f"  {linha.nome:<20} | {linha.departamento:<12} | gestor: {linha.gestor} | orçamento: {linha.orcamento}")

# Left join — mantém todos os colaboradores, mesmo sem departamento correspondente
# Para demonstrar, adicionamos um colaborador sem departamento cadastrado
from icsv import ICSV, Linha
colaboradores_extra = colaboradores + ICSV(texto="nome;email;cidade;departamento;cargo;salario;status\nXisto Leal;xisto@empresa.com;Manaus;Juridico;Advogado;9000;ativo", possui_cabecalho=True)

resultado_left = colaboradores_extra.join(departamentos, chave_esq='departamento', chave_dir='departamento', tipo='left')
print(f"\n--- LEFT JOIN (com departamento sem correspondência) ---")
print(f"Linhas: {len(resultado_left)}")

sem_gestor = resultado_left.filtrar_por_coluna('gestor', '')
print(f"Sem gestor correspondente: {len(sem_gestor)}")
for linha in sem_gestor:
    print(f"  {linha.nome} — {linha.departamento}")
