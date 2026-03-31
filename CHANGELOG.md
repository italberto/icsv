# Changelog

Todas as mudanças notáveis deste projeto são documentadas neste arquivo.

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

---

## [1.1.0] — 2026-03-31

### Adicionado

- Acesso a campos de `Linha` por subscript: `linha["nome"]` (string, insensível a maiúsculas) e `linha[0]` (índice inteiro), complementando o acesso por atributo já existente (`linha.nome`). Colunas com nomes que contêm hífens, espaços ou outros caracteres inválidos como identificador Python agora são acessíveis via subscript.

### Corrigido

- **Bug — encoding `utf-8-sig`:** arquivos com BOM (`\ufeff`) eram decodificados pelo codec `utf-8` antes de `utf-8-sig` ter chance de ser tentado. Como `utf-8` aceita o BOM sem erro, o caractere vazava para o nome da primeira coluna (ex.: `'\ufeffnome'` em vez de `'nome'`). Corrigido invertendo a prioridade: `utf-8-sig` é agora tentado antes de `utf-8` na detecção automática de encoding. Ambos os codecs funcionam corretamente em arquivos sem BOM, tornando a mudança segura e retrocompatível.
- **Documentação — `tratamento_linhas_irregulares`:** o changelog e a docstring da versão 1.0.0 listavam os valores `'ignorar'`, `'normalizar'` e `'erro'`, que nunca foram implementados. Os únicos valores aceitos são `'avisar'` (padrão) e `'preencher'`.
- **Documentação — `agrupar_por`:** o método foi listado no changelog da versão 1.0.0 mas nunca foi implementado no código. Entrada removida da documentação.

### Performance

- Mapa de colunas (`{nome_coluna: índice}`) calculado uma única vez por iteração em `__iterar_linhas_parseadas`, em vez de ser recriado para cada linha. A referência ao dicionário é reutilizada por todas as instâncias de `Linha`, eliminando alocações desnecessárias em arquivos com muitas linhas.

### Testes

Suite expandida de 105 para 147 testes (+42). Novas classes e casos adicionados:

- **`TestDetectarQuebraDeLinha`** — cobertura da função `detectar_quebra_de_linha_arquivo`: detecção de CRLF, LF, CR e `INDETERMINADO` em arquivo real.
- **`TestSalvarFiltradoPorRegex`** — cobertura de `salvar_filtrado_por_regex`: gravação incremental, sem match (cabeçalho gravado, 0 linhas), contagem de retorno.
- **`TestJoinLeft`** — cobertura do join `left`: linha sem match mantida com campos vazios, todos com match, sem nenhum match, tipo inválido, ausência de cabeçalho.
- **`TestFiltrarPorColunaWildcards`** — cobertura dos padrões `texto%`, `%texto`, `%texto%`, `ignorar_maiusculas=False`, ausência de cabeçalho.
- **`TestEncodings`** — cobertura do encoding `utf-8-sig` (arquivo com BOM).
- **`TestDelimitadores`** — cobertura de delimitadores `\t` (tab) e `|` (pipe), incluindo acesso por atributo.
- **`TestStreamComArquivo`** — cobertura de `modo_leitura='stream'` com arquivo real em disco (os testes anteriores usavam apenas `texto=`), incluindo `getattr` e filtragem lazy.
- **`TestOrderByReverse`** — cobertura de `order_by_field_name` e `order_by_field_index` com `reverse=True`.
- **`TestJoinSufixos`** — cobertura de colunas com nomes conflitantes nos dois CSVs (sufixos padrão `_esq`/`_dir`) e sufixos customizados.
- **`TestEdgeCases`** — edge cases: CSV só com cabeçalho (sem dados), `head`/`tail` com `n` maior que o total de linhas, quoting automático de campos contendo o delimitador, `valores_unicos`/`contar_por`/`deduplicar`/`filtrar_por_regex` sem cabeçalho, join left sem nenhum match.
- **`TestLinha`** — testes de `__getitem__` por índice, nome e com case-insensitive.

---

## [1.0.0] — 2026-03-31

### Adicionado

#### Leitura e carregamento
- Carregamento de CSV a partir de arquivo em disco, string (`texto=`) ou lista de dicts (`from_list_of_dicts`).
- Detecção automática de delimitador (`,` `;` `\t` `|`), cabeçalho e quebra de linha.
- Parâmetros explícitos: `delimitador`, `possui_cabecalho`, `encoding`, `quebra_linha`.
- Dois modos de leitura: `modo_leitura='eager'` (padrão, tudo em memória) e `modo_leitura='stream'` (linha a linha, sem materializar).

#### Inspeção
- `info()` e `info_json()` — resumo do arquivo com colunas, tipos inferidos e contagem de linhas.
  - Em modo `stream`, retornam `"Desconhecido (modo stream)"` para evitar varredura completa.
- `preview()` — cabeçalho + primeiras 5 linhas.
- `len(dados)` — número exato de linhas (varredura completa em stream).
- `len_stream_estimate(amostra_linhas, retornar_intervalo, confianca)` — estimativa sem varredura completa:
  - Baseada em tamanho do arquivo e tamanho médio de amostra de linhas.
  - `retornar_intervalo=True` retorna também a faixa `(min, max)`.
  - `confianca` ajusta a largura do intervalo (0.80 a 0.99, padrão 0.95).

#### Acesso e iteração
- Acesso por índice (`dados[0]`) e slice (`dados[0:5]`).
- Acesso a campos por nome de atributo (`linha.nome`, `linha.cpf`).
- Iteração com `for linha in dados`.
- `head(n)` — primeiras n linhas (novo ICSV).
- `tail(n)` — últimas n linhas (novo ICSV), otimizado com `collections.deque(maxlen=n)` em stream.

#### Filtragem
- `filtrar_por_coluna(col, padrão)` — filtra com suporte a `%texto%`, `texto%`, `%texto`.
- `filtrar_por_regex(col, regex)` — filtra por expressão regular.
- `iter_filtrar_por_coluna(col, padrão)` — versão lazy (iterador), sem materializar resultado.
- `iter_filtrar_por_regex(col, regex)` — versão lazy por regex.
- `salvar_filtrado_por_coluna(caminho, col, padrão)` — filtra e grava incrementalmente; retorna total de linhas salvas.
- `salvar_filtrado_por_regex(caminho, col, regex)` — idem, por regex.

#### Manipulação
- `adicionar_coluna(nome, valor_padrao)` — acrescenta coluna ao ICSV.
- `remover_coluna(nome)` — remove coluna pelo nome.
- `renomear_coluna(de, para)` — renomeia coluna.
- `selecionar_colunas(lista)` — retorna novo ICSV com subconjunto de colunas.
- `adicionar_linha(valores)` — insere nova linha.
- `remover_linha(indice)` — remove linha por índice.
- `modificar_campo(indice_linha, nome_coluna, novo_valor)` — altera valor de célula.
- `aplicar_em_coluna(nome_coluna, funcao)` — aplica função sobre todos os valores de uma coluna.

#### Ordenação, agrupamento e deduplicação
- `order_by_field_name(col, reverse, cast_type)` — ordenação in-place.
- `contar_por(col)` — dicionário com frequência de valores por coluna.
- `deduplicar(col)` — remove duplicatas por coluna; retorna novo ICSV.

#### Combinação
- `concatenar(outro)` / `dados1 + dados2` — une dois CSVs com mesmo cabeçalho.
- `join(outro, chave_esq, chave_dir, tipo, sufixos)` — hash join inner ou left entre dois ICSVs.

#### Exportação
- `salvar()` — salva no caminho original.
- `salvar_como(novo_caminho)` — salva em novo arquivo.
- `to_list_of_dicts()` — converte para lista de dicionários.
- `to_json()` — representação JSON do objeto.

#### Resiliência
- `tratamento_linhas_irregulares` — comportamento configurável para linhas com número incorreto de campos: `'avisar'` (padrão) e `'preencher'`.

### Documentação
- README com referência completa da API, dicas para arquivos > 1 GB e boas práticas.
- Seção de **Limitações** documentando:
  - Hash Join: arquivo da direita carregado em memória (colocar o menor à direita).
  - `len_stream_estimate`: imprecisão em campos CSV com quebras de linha internas (campos multilinhas entre aspas).
- 13 exemplos comentados em `examples/` cobrindo leitura básica, filtros, manipulação, análise, join, exportação, stream e estimativas.

### Testes
- Suite com 105 testes cobrindo modos eager e stream, filtragem lazy, estimativas com intervalos de confiança, join, exportação e resiliência a linhas irregulares.

[1.0.0]: https://github.com/italberto/icsv/releases/tag/v1.0.0
