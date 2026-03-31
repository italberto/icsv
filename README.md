# icsv

[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![GitHub last commit](https://img.shields.io/github/last-commit/italberto/icsv)](https://github.com/italberto/icsv/commits/main)
[![GitHub repo size](https://img.shields.io/github/repo-size/italberto/icsv)](https://github.com/italberto/icsv)

Biblioteca Python leve, de arquivo único, para leitura, manipulação e análise de arquivos CSV.

## Características

- **Arquivo único** — apenas copie `icsv.py` para o seu projeto
- **Sem dependências externas** — usa somente a biblioteca padrão do Python
- **Detecção automática** de delimitador, cabeçalho e quebra de linha
- **Acesso por nome de coluna** — `linha.nome`, `linha.cpf` ou `linha["nome"]`, `linha["cpf"]`
- **Filtros estilo SQL LIKE** — `%texto%`, `texto%`, `%texto`
- **JOIN** entre arquivos (inner e left)
- **Operações não destrutivas** — `filtrar`, `head`, `tail`, `selecionar_colunas`, `deduplicar` retornam novo ICSV
- **Integração com ecossistema Python** — `to_list_of_dicts`, `from_list_of_dicts`

## Requisitos

Python 3.8+. Nenhuma dependência externa.

## Instalação

Não há pacote no PyPI. Copie `icsv.py` para o seu projeto:

```bash
curl -O https://raw.githubusercontent.com/seu-usuario/icsv/main/icsv.py
```

## Início rápido

```python
from icsv import ICSV

dados = ICSV('colaboradores.csv')

print(dados.info())
print(dados.preview())

# Acesso por nome de coluna
for linha in dados:
    print(linha.nome, linha.departamento)

# Filtro e contagem
ti = dados.filtrar_por_coluna('departamento', 'TI')
print(dados.contar_por('departamento'))
```

**Dica rápida para arquivos grandes (>1GB):**

- Abra com `modo_leitura='stream'`.
- Prefira `iter_filtrar_por_coluna` / `iter_filtrar_por_regex` para processamento lazy.
- Para exportar subset grande, use `salvar_filtrado_por_coluna` / `salvar_filtrado_por_regex`.
- Para estimar volume, use `len_stream_estimate()` em vez de `len(dados)`.
- Evite `to_json()` e `to_list_of_dicts()` em dumps grandes.

## Referência da API

### Carregamento

```python
ICSV('arquivo.csv')                          # a partir de arquivo
ICSV(texto="nome,idade\nAna,30")             # a partir de string
ICSV.from_list_of_dicts([{"nome": "Ana"}])   # a partir de lista de dicts
```

Os parâmetros `delimitador`, `possui_cabecalho` e `quebra_linha` são detectados automaticamente, mas podem ser informados:

```python
ICSV('arquivo.csv', delimitador=';', possui_cabecalho=True, encoding='latin-1')
```

Para controlar uso de memória em arquivos muito grandes:

```python
# padrão: carrega tudo em memória
ICSV('arquivo.csv', modo_leitura='eager')

# leitura sob demanda (streaming)
ICSV('arquivo.csv', modo_leitura='stream')
```

### Semântica do modo stream

- O objeto `ICSV` em `modo_leitura='stream'` pode ser iterado mais de uma vez.
  Cada novo `for linha in dados` reabre a fonte e recomeça do início.
- Já os iteradores retornados por `iter(dados)`, `iter_filtrar_por_coluna()` e
  `iter_filtrar_por_regex()` são de consumo único, como qualquer iterador Python.
- Em `stream`, `len(dados)` funciona, mas faz uma varredura completa a cada chamada.
- Em `stream`, acesso aleatório não existe: `dados[0]`, `dados[-1]` e fatias
  arbitrárias exigiriam materialização ou indexação prévia.
- Para primeiras linhas, use `head(n)` ou `dados[:n]`.
- Para últimas linhas, use `tail(n)`; em `stream`, isso exige ler o arquivo inteiro,
  mantendo apenas as últimas `n` linhas em memória.

Para CSVs corrompidos com linhas de tamanho diferente do cabeçalho, use `tratamento_linhas_irregulares`:

```python
# padrão: avisa durante o parse e mantém a linha original
ICSV('arquivo.csv', tratamento_linhas_irregulares='avisar')

# normaliza durante o parse:
# - linha menor: preenche com ""
# - linha maior: trunca para o tamanho do cabeçalho
ICSV('arquivo.csv', tratamento_linhas_irregulares='preencher')
```

---

### Inspeção

| Operação | Descrição |
|---|---|
| `len(dados)` | Número de linhas; em `stream`, faz varredura completa |
| `dados[0]` | Linha por índice (`Linha`); indisponível em `stream` |
| `dados[:5]` | Prefixo — retorna novo `ICSV`; forma recomendada em `stream` |
| `dados.info()` | Informações em texto |
| `dados.info_json()` | Informações em JSON |
| `dados.len_stream_estimate(amostra_linhas=1000)` | Estimativa rápida de linhas (sem varredura completa em stream) |
| `dados.len_stream_estimate(..., retornar_intervalo=True, confianca=0.95)` | Estimativa + intervalo com nível de confiança (0.80–0.99) |
| `dados.preview()` | Cabeçalho + primeiras 5 linhas |
| `dados.cabecalho.campos` | Lista de nomes das colunas |

No `modo_leitura='stream'`, `dados[-1]` não é permitido porque acesso aleatório ao
fim do arquivo exigiria materializar tudo ou manter um índice completo em memória.
Use `tail(1)` quando quiser a última linha.

---

### Acesso a campos de uma linha

Dentro de um laço `for linha in dados`, os campos de cada `Linha` podem ser acessados de três formas:

| Forma | Exemplo | Notas |
|---|---|---|
| Atributo | `linha.nome` | Conveniente; requer nome válido como identificador Python |
| Subscript por nome | `linha["nome"]` | Funciona com qualquer nome, incluindo hífens e espaços; insensível a maiúsculas |
| Subscript por índice | `linha[0]` | Acesso posicional; lança `IndexError` se fora do intervalo |

```python
for linha in dados:
    print(linha.nome)         # atributo
    print(linha["nome"])      # subscript por nome (equivalente)
    print(linha["Nome"])      # case-insensitive
    print(linha[0])           # primeiro campo
```

---

### Filtragem e ordenação

| Método | Descrição |
|---|---|
| `filtrar_por_coluna(col, padrão)` | Filtra com suporte a `%` — retorna novo `ICSV` |
| `iter_filtrar_por_coluna(col, padrão)` | Filtragem lazy — retorna iterador de `Linha` |
| `filtrar_por_regex(col, regex)` | Filtra por regex — retorna novo `ICSV` |
| `iter_filtrar_por_regex(col, regex)` | Filtragem regex lazy — retorna iterador de `Linha` |
| `order_by_field_name(col, reverse, cast_type)` | Ordenação in-place |
| `head(n=5)` | Primeiras n linhas → novo `ICSV` |
| `tail(n=5)` | Últimas n linhas → novo `ICSV`; em `stream`, faz varredura completa |

No `modo_leitura='stream'`, os métodos `filtrar_por_*` ainda retornam um novo objeto materializado em memória.
Para processamento realmente incremental, prefira `iter_filtrar_por_*` ou `salvar_filtrado_por_*`.

**Padrões de filtro:**

| Padrão | Comportamento |
|---|---|
| `'Ana'` | Correspondência exata |
| `'Ana%'` | Começa com "Ana" |
| `'%Silva'` | Termina com "Silva" |
| `'%an%'` | Contém "an" (case-insensitive por padrão) |

---

### Análise

| Método | Descrição |
|---|---|
| `valores_unicos(col)` | `set` com valores distintos da coluna |
| `contar_por(col)` | `dict` com contagem por valor |
| `deduplicar(coluna=None)` | Remove duplicatas → novo `ICSV` |

---

### Manipulação de colunas

| Método | Descrição |
|---|---|
| `selecionar_colunas(["a", "b"])` | Projeção de colunas → novo `ICSV` |
| `adicionar_coluna(nome, valor_padrão='')` | In-place |
| `remover_coluna(nome)` | In-place |
| `atualizar_nome_coluna(antigo, novo)` | In-place |
| `modificar_valores(col, func, col_param=None)` | Aplica função in-place |

---

### Manipulação de linhas

| Método | Descrição |
|---|---|
| `adicionar_linha(Linha)` | In-place |
| `remover_linha(indice)` | In-place |

---

### Combinação

| Método | Descrição |
|---|---|
| `concatenar(outro)` ou `csv1 + csv2` | Une dois CSVs com mesmo cabeçalho |
| `join(outro, chave_esq, chave_dir, tipo='inner')` | JOIN inner ou left |

---

### Exportação

| Método | Descrição |
|---|---|
| `salvar()` | Salva no caminho original |
| `salvar_como(novo_caminho)` | Salva em novo arquivo |
| `salvar_filtrado_por_coluna(caminho, col, padrão)` | Filtra e grava incrementalmente (sem materializar tudo) |
| `salvar_filtrado_por_regex(caminho, col, regex)` | Filtra por regex e grava incrementalmente |
| `to_list_of_dicts()` | `[{"col": "valor", ...}, ...]` |
| `to_json()` | Representação JSON completa do objeto |

Exemplo de filtro incremental por regex (ideal para arquivos grandes):

```python
dados = ICSV("log.csv", possui_cabecalho=True, modo_leitura="stream")
total = dados.salvar_filtrado_por_regex("erros_5xx.csv", "status", r"^5\d\d$")
print(total)
```

Exemplo de pipeline incremental com múltiplos filtros:

```python
import re

dados = ICSV("log.csv", possui_cabecalho=True, modo_leitura="stream")
iter_5xx = dados.iter_filtrar_por_regex("status", r"^5\d\d$")
iter_final = (linha for linha in iter_5xx if re.search(r"timeout|unavailable", linha.mensagem, re.IGNORECASE))

for linha in iter_final:
    ...  # processa ou grava incrementalmente
```

### Boas práticas para arquivos > 1GB

- Use `modo_leitura='stream'` ao abrir arquivos grandes.
- Prefira `iter_filtrar_por_coluna` / `iter_filtrar_por_regex` para processamento lazy.
- Para exportar subset grande, use `salvar_filtrado_por_coluna` / `salvar_filtrado_por_regex`.
- Evite `to_json()` e `to_list_of_dicts()` em dumps muito grandes, pois materializam os dados.
- Em `stream`, trate `len(dados)` como operação custosa (varredura completa).

---

## Limitações

### Hash Join — Restrição de Memória

O método `join()` utiliza **hash join**, que carrega o arquivo da direita (`outro_csv`) inteiramente em memória para construir um índice de O(1) lookup.

**Problema:** Se o arquivo da direita for muito grande (ex: > 2 GB), pode ocorrer `MemoryError`.

**Solução:**
- Sempre coloque o arquivo **menor** no lado direito (parâmetro `outro_csv`).
- Se ambos forem grandes, filtre colunas desnecessárias antes do join:
  ```python
  clientes_pequeno = clientes.selecionar_colunas(['id', 'nome'])
  resultado = pedidos.join(clientes_pequeno, 'id_cliente', 'id')
  ```
- Para arquivos muito grandes, considere particionar os dados ou usar ferramentas especializadas (DuckDB, Polars).

### len_stream_estimate() — Campos Multilinhas

O método `len_stream_estimate()` estima o número de linhas dividindo o tamanho do arquivo pelo tamanho médio de uma linha. A estimativa assume que cada `\n` marca o fim de uma linha.

**Problema:** Se o CSV contiver campos entre aspas com quebras de linha internas (ex: comentários multilinhas em registros Moodle, descrições de chamados), o método conta `\n` errado:
- Cada quebra dentro de um campo gera um falso "fim de linha"
- O tamanho médio de linha aumenta, causando desvio padrão mais alto
- Os intervalos de confiança ficam mais largos, reduzindo a precisão

**Exemplo problemático:**
```csv
id,descricao
1,"Bug em autenticação
Testado em Chrome e Firefox
Ocorre em todos os navegadores"
2,"Funciona normalmente"
```

**Solução:**
- Para arquivos com muitos campos multilinhas, use `len(dados)` em modo eager (varredura completa) ou stream (varredura).
- Se for crítico evitar varredura, filtre os campos multilinhas antes de estimar:
  ```python
  # Carregar com lazy (stream), depois chamar len() se preciso de exatidão
  dados = ICSV("arquivo.csv", modo_leitura="stream")
  total = len(dados)  # Varredura completa, mas valor exato
  ```

### Stream — Iteração vs. acesso aleatório

O `ICSV` em `stream` **não vira um objeto esgotado** depois de um `for linha in dados`.
Uma nova iteração sobre `dados` reabre a fonte e começa novamente do início.

```python
dados = ICSV("arquivo.csv", modo_leitura="stream")

for linha in dados:
    ...

# funciona: nova varredura desde o início
total = len(dados)

# funciona: nova iteração desde o início
for linha in dados:
    ...
```

Já um iterador obtido explicitamente é de consumo único:

```python
it = dados.iter_filtrar_por_coluna("status", "5%")
list(it)   # consome
list(it)   # agora retorna vazio
```

Além disso, em `stream` não há acesso aleatório eficiente. Por isso:

- `dados[0]` e `dados[-1]` lançam erro
- `dados[:n]` é apropriado para prefixos
- `tail(n)` funciona, mas precisa percorrer todo o arquivo
- para acesso indexado repetido, reabra em `modo_leitura='eager'`

---

## Exemplos

A pasta [`examples/`](examples/) contém exemplos completos e comentados:

| Arquivo | Conteúdo |
|---|---|
| `01_leitura_basica.py` | Carregar, inspecionar e iterar |
| `02_filtros.py` | Filtros LIKE e ordenação |
| `03_manipulacao.py` | Adicionar/remover/modificar colunas e linhas |
| `04_analise.py` | Análise, agrupamento, slicing e deduplicação |
| `05_join.py` | JOIN entre dois arquivos |
| `06_exportacao.py` | Salvar, exportar e importar dados |
| `08_linhas_irregulares.py` | Aviso padrão e normalização com `tratamento_linhas_irregulares` |
| `09_stream_grandes_arquivos.py` | Processamento de arquivos grandes com `modo_leitura='stream'` |
| `10_stream_filtro_para_arquivo.py` | Filtro incremental com `salvar_filtrado_por_coluna` |
| `11_stream_regex_para_arquivo.py` | Filtro incremental por regex com `salvar_filtrado_por_regex` |
| `12_stream_pipeline_multiplos_filtros.py` | Pipeline incremental com múltiplos filtros (ETL leve) |
| `13_len_stream_estimate.py` | Comparação entre `len_stream_estimate()` e `len()` em stream |

Para executar a partir da raiz do projeto:

```bash
python examples/01_leitura_basica.py
```

---

## Testes

```bash
pip install pytest
pytest test_icsv.py -v
```

---

## Changelog

Consulte o arquivo [CHANGELOG.md](CHANGELOG.md) para o histórico completo de mudanças.

---

## Licença

MIT — consulte o arquivo [LICENSE](LICENSE).
