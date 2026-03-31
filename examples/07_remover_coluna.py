"""
Exemplo 07 — Remover coluna
Demonstra remoção de coluna em um CSV.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV

colaboradores = ICSV(os.path.join(os.path.dirname(__file__), 'colaboradores.csv'))

print(colaboradores.preview())

colaboradores.remover_coluna("cidade")

print(colaboradores.preview())

