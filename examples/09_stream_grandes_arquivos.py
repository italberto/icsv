"""
Exemplo 09 — Stream para arquivos grandes
Demonstra como usar modo_leitura='stream' para processar arquivos sem
carregar inteiro na memória, ideal para logs de 5GB+.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icsv import ICSV

# Simula um arquivo grande com muitas linhas
csv_grande = "timestamp,ip,url,status,bytes\n"
for i in range(100000):
    csv_grande += f"2026-03-31T{i%24:02d}:00:00,192.168.1.{i%255},/api/users/{i%1000},200,{1024 + i%10000}\n"

# Salvar em arquivo temporário
temp_arquivo = "/tmp/access_log_grande.csv"
with open(temp_arquivo, 'w') as f:
    f.write(csv_grande)

print(f"✓ Arquivo criado: {temp_arquivo} ({os.path.getsize(temp_arquivo) / 1024 / 1024:.2f} MB)")

# === EAGER (carrega tudo em memória) ===
print("\n=== Modo EAGER ===")
dados_eager = ICSV(temp_arquivo, possui_cabecalho=True, modo_leitura="eager")
print(f"Carregado: {len(dados_eager)} linhas em memória")

# Filtro e estatística em eager
print(f"  Status 200: {len([l for l in dados_eager if l.status == '200'])} linhas")

# === STREAM (leitura incremental) ===
print("\n=== Modo STREAM ===")
dados_stream = ICSV(temp_arquivo, possui_cabecalho=True, modo_leitura="stream")

# Operações que não materializam tudo:
# 1) Iteração e coleta sob demanda
print("  Processando com stream...")
status_200 = 0
for linha in dados_stream:
    if linha.status == "200":
        status_200 += 1
    if status_200 % 25000 == 0 and status_200 > 0:
        print(f"    ... {status_200} linhas processadas")

print(f"  Status 200 (contados em stream): {status_200} linhas")

# 2) Filtro retorna novo ICSV (materialiando apenas subset)
print("\n  Filtrando para apenas IPs de 192.168.1.1 (stream-aware)...")
dados_stream2 = ICSV(temp_arquivo, possui_cabecalho=True, modo_leitura="stream")
ips_especificos = dados_stream2.filtrar_por_coluna("ip", "192.168.1.1")
print(f"  Resultado: {len(ips_especificos)} linhas (carregadas em memory)")

# 3) Head em stream (pega apenas N primeiras)
print("\n  Primeiras 10 linhas (head)...")
dados_stream3 = ICSV(temp_arquivo, possui_cabecalho=True, modo_leitura="stream")
preview = dados_stream3.head(10)
print(preview)

# Cleanup
os.unlink(temp_arquivo)
print(f"\n✓ Arquivo temporário removido")
