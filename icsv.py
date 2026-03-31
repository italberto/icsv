"""
icsv — Biblioteca Python leve para leitura, manipulação e análise de arquivos CSV.

Arquivo único, sem dependências externas. Requer Python 3.8+.

Uso básico:
    from icsv import ICSV

    dados = ICSV('arquivo.csv')
    for linha in dados:
        print(linha.nome, linha.status)

    ativos = dados.filtrar_por_coluna('status', 'ativo')
    dados.salvar_como('saida.csv')
"""

from __future__ import annotations

import csv
import json
import io
import os
from collections import deque
import itertools
import re
import statistics
import warnings
from enum import Enum
from typing import Any, Callable, Iterable, Iterator


class Quebra(Enum):
    """Tipos de quebra de linha reconhecidos pela biblioteca.

    Valores:
        CRLF: Sequência Windows (\\r\\n).
        LF: Sequência Unix/Linux/macOS moderno (\\n).
        CR: Sequência Mac clássico (\\r).
        INDETERMINADO: Nenhuma quebra detectada na amostra.
    """
    CRLF = "\r\n"
    LF = "\n"
    CR = "\r"
    INDETERMINADO = None


class TipoQuebraSO(Enum):
    """Mapeamento entre sistemas operacionais e seus tipos de quebra de linha padrão."""
    WINDOWS = Quebra.CRLF
    UNIX = Quebra.LF
    MAC = Quebra.CR


def detectar_quebra_de_linha_arquivo(caminho: str) -> Quebra:
    """Detecta o tipo de quebra de linha presente em um arquivo.

    Lê os primeiros 1024 bytes do arquivo em modo binário para identificar
    a sequência de quebra de linha sem interferência do modo texto do Python.

    Args:
        caminho: Caminho absoluto ou relativo para o arquivo.

    Returns:
        Membro de `Quebra` correspondente ao tipo detectado,
        ou `Quebra.INDETERMINADO` se nenhum for encontrado.
    """
    with open(caminho, 'rb') as f:
        amostra = f.read(1024)
        if b'\r\n' in amostra:
            return Quebra.CRLF
        elif b'\n' in amostra:
            return Quebra.LF
        elif b'\r' in amostra:
            return Quebra.CR
        else:
            return Quebra.INDETERMINADO


def detectar_quebra_de_linha_texto(texto: str) -> Quebra:
    """Detecta o tipo de quebra de linha presente em uma string.

    Args:
        texto: String a ser analisada.

    Returns:
        Membro de `Quebra` correspondente ao tipo detectado,
        ou `Quebra.INDETERMINADO` se nenhum for encontrado.
    """
    if '\r\n' in texto:
        return Quebra.CRLF
    elif '\n' in texto:
        return Quebra.LF
    elif '\r' in texto:
        return Quebra.CR
    else:
        return Quebra.INDETERMINADO


class Linha:
    """Representa uma linha de dados de um arquivo CSV.

    Armazena os campos como strings e permite acesso posicional ou por nome
    de coluna. O acesso por nome depende de um `mapa_colunas` injetado
    externamente pela classe `ICSV` após a leitura do cabeçalho.

    Exemplo:
        linha = Linha(['Ana', '30', 'SP'], mapa_colunas={'nome': 0, 'idade': 1, 'estado': 2})
        print(linha.nome)   # 'Ana'
        print(linha[1])     # acesso via obter_campo(1) → '30'
    """

    def __init__(
        self,
        campos: list[str] | None = None,
        delimitador: str = ",",
        quebra_linha: str = "\n",
        mapa_colunas: dict[str, int] | None = None,
    ) -> None:
        """Inicializa uma linha CSV.

        Args:
            campos: Lista de strings com os valores de cada campo.
                    Se None, cria uma linha vazia.
            delimitador: Caractere separador de campos (padrão: vírgula).
            quebra_linha: Sequência de fim de linha (padrão: '\\n').
            mapa_colunas: Dicionário `{nome_coluna_lower: indice}` que
                          habilita o acesso por atributo (ex.: `linha.cpf`).
                          Normalmente injetado por `ICSV` — não é necessário
                          fornecer manualmente.
        """
        self.__campos: list[str] = list(campos) if campos else []
        self.__delimitador: str = delimitador
        self.__quebra_linha: str = quebra_linha
        self.__mapa_colunas: dict[str, int] = self.__sanitizar_mapa_colunas(mapa_colunas or {})

    @staticmethod
    def __sanitizar_mapa_colunas(mapa: dict[str, int]) -> dict[str, int]:
        """Normaliza chaves do mapa para acesso por atributo em O(1)."""
        return {str(nome).strip().lower(): indice for nome, indice in mapa.items()}

    # --- Properties ---

    @property
    def campos(self) -> list[str]:
        """Lista mutável com os valores de cada campo da linha."""
        return self.__campos

    @property
    def delimitador(self) -> str:
        """Caractere usado para separar os campos."""
        return self.__delimitador

    @property
    def quebra_linha(self) -> str:
        """Sequência de fim de linha associada a esta linha."""
        return self.__quebra_linha

    @property
    def mapa_colunas(self) -> dict[str, int]:
        """Dicionário `{nome_coluna_lower: indice}` usado pelo acesso por atributo."""
        return self.__mapa_colunas

    @mapa_colunas.setter
    def mapa_colunas(self, novo_mapa: dict[str, int]) -> None:
        self.__mapa_colunas = self.__sanitizar_mapa_colunas(novo_mapa)

    @quebra_linha.setter
    def quebra_linha(self, nova_quebra: str) -> None:
        self.__quebra_linha = nova_quebra

    @delimitador.setter
    def delimitador(self, novo_delimitador: str) -> None:
        self.__delimitador = novo_delimitador

    # --- Manipulação de campos ---

    def adicionar_campo(self, campo: str) -> None:
        """Adiciona um campo ao final da linha.

        Args:
            campo: Valor do novo campo.
        """
        self.__campos.append(campo)

    def remover_campo_por_indice(self, indice: int) -> None:
        """Remove o campo na posição indicada.

        Args:
            indice: Posição zero-based do campo a remover.

        Raises:
            IndexError: Se `indice` estiver fora do intervalo válido.
        """
        if 0 <= indice < len(self.__campos):
            self.__campos.pop(indice)
        else:
            raise IndexError("Índice fora do intervalo da linha.")

    def atualizar_campo(self, indice: int, novo_campo: str) -> None:
        """Substitui o valor do campo na posição indicada.

        Args:
            indice: Posição zero-based do campo a atualizar.
            novo_campo: Novo valor a atribuir.

        Raises:
            IndexError: Se `indice` estiver fora do intervalo válido.
        """
        if 0 <= indice < len(self.__campos):
            self.__campos[indice] = novo_campo
        else:
            raise IndexError("Índice fora do intervalo da linha.")

    def obter_campo(self, indice: int) -> str:
        """Retorna o valor do campo na posição indicada.

        Args:
            indice: Posição zero-based do campo.

        Returns:
            Valor do campo como string.

        Raises:
            IndexError: Se `indice` estiver fora do intervalo válido.
        """
        if 0 <= indice < len(self.__campos):
            return self.__campos[indice]
        else:
            raise IndexError("Índice fora do intervalo da linha.")

    def numero_de_campos(self) -> int:
        """Retorna a quantidade de campos presentes na linha."""
        return len(self.__campos)

    def limpar_campos(self) -> None:
        """Remove todos os campos da linha, deixando-a vazia."""
        self.__campos.clear()

    def existe_campo(self, campo: str) -> bool:
        """Verifica se um valor exato existe entre os campos da linha.

        Args:
            campo: Valor a procurar.

        Returns:
            True se o valor for encontrado, False caso contrário.
        """
        return campo in self.__campos

    def definir_mapa_colunas(self, mapa: dict[str, int]) -> None:
        """Injeta ou substitui o mapa de colunas desta linha.

        Chamado internamente por `ICSV` sempre que o cabeçalho é criado
        ou alterado, garantindo que o acesso por atributo reflita a
        estrutura atual do arquivo.

        Args:
            mapa: Dicionário `{nome_coluna: indice}` (normalizado internamente).
        """
        self.__mapa_colunas = self.__sanitizar_mapa_colunas(mapa)

    # --- Serialização ---

    def to_dict(self) -> dict[str, Any]:
        """Converte a linha para um dicionário serializável.

        Returns:
            Dicionário com as chaves `campos`, `delimitador` e `quebra_linha`.
        """
        return {
            "campos": self.__campos,
            "delimitador": self.__delimitador,
            "quebra_linha": self.__quebra_linha,
        }

    def to_json(self) -> str:
        """Serializa a linha para JSON.

        Returns:
            String JSON equivalente ao resultado de `to_dict()`.
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Linha:
        """Cria uma instância de `Linha` a partir de um dicionário.

        Args:
            data: Dicionário com as chaves `campos`, `delimitador` e
                  `quebra_linha`. Chaves ausentes assumem valores padrão.

        Returns:
            Nova instância de `Linha`.
        """
        return cls(
            campos=data.get("campos", []),
            delimitador=data.get("delimitador", ","),
            quebra_linha=data.get("quebra_linha", "\n"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> Linha:
        """Cria uma instância de `Linha` a partir de uma string JSON.

        Args:
            json_str: String JSON gerada por `to_json()`.

        Returns:
            Nova instância de `Linha`.
        """
        return cls.from_dict(json.loads(json_str))

    # --- Dunder methods ---

    def __getattr__(self, nome_atributo: str) -> str:
        """Permite acesso aos campos por nome de coluna como atributo.

        Quando um atributo não existe na instância, o Python invoca este
        método automaticamente. Se o nome corresponder a uma coluna no
        `mapa_colunas`, o valor do campo é retornado diretamente.

        Args:
            nome_atributo: Nome do atributo solicitado.

        Returns:
            Valor do campo correspondente à coluna de mesmo nome.

        Raises:
            AttributeError: Se `nome_atributo` não corresponder a nenhuma
                            coluna registrada no `mapa_colunas`.

        Exemplo:
            linha.cpf      # equivale a linha.obter_campo(mapa_colunas['cpf'])
            linha.status   # idem para a coluna 'status'
        """
        if nome_atributo in self.__mapa_colunas:
            return self.obter_campo(self.__mapa_colunas[nome_atributo])
        raise AttributeError(f"A linha não possui a coluna ou atributo '{nome_atributo}'")

    def __eq__(self, other: object) -> bool:
        """Compara duas linhas por valor, delimitador e quebra de linha."""
        if isinstance(other, Linha):
            return (
                self.__campos == other.campos
                and self.__delimitador == other.delimitador
                and self.__quebra_linha == other.quebra_linha
            )
        return False

    def __str__(self) -> str:
        """Retorna a representação CSV da linha, sem a quebra de linha final."""
        saida_em_memoria = io.StringIO()
        escritor = csv.writer(saida_em_memoria, delimiter=self.__delimitador)
        escritor.writerow(self.__campos)
        return saida_em_memoria.getvalue().strip('\r\n')

    def __repr__(self) -> str:
        return (
            f"Linha(campos={self.__campos}, "
            f"delimitador='{self.__delimitador}', "
            f"quebra_linha='{self.__quebra_linha}')"
        )


class Cabecalho(Linha):
    """Representa o cabeçalho de um arquivo CSV — a linha com os nomes das colunas.

    Herda de `Linha` e adiciona validação de nomes duplicados e busca
    de índice por nome de coluna. É criado automaticamente por `BaseICSV`
    ao ler a primeira linha de um arquivo com cabeçalho.
    """

    def __init__(
        self,
        campos: list[str] | None = None,
        delimitador: str = ",",
        quebra_linha: str = "\n",
    ) -> None:
        """Inicializa o cabeçalho e valida a unicidade dos nomes de coluna.

        Args:
            campos: Lista com os nomes das colunas.
            delimitador: Caractere separador de campos (padrão: vírgula).
            quebra_linha: Sequência de fim de linha (padrão: '\\n').

        Raises:
            ValueError: Se dois ou mais campos tiverem o mesmo nome.
        """
        super().__init__(campos, delimitador, quebra_linha)
        self.__validar_duplicatas()

    def __validar_duplicatas(self) -> None:
        """Lança ValueError se houver nomes de colunas repetidos."""
        if len(self.campos) != len(set(self.campos)):
            raise ValueError("O cabeçalho contém nomes de colunas duplicados!")

    def obter_indice_da_coluna(self, nome_coluna: str) -> int:
        """Retorna o índice zero-based de uma coluna pelo seu nome.

        Args:
            nome_coluna: Nome exato da coluna (sensível a maiúsculas).

        Returns:
            Índice inteiro da coluna.

        Raises:
            ValueError: Se a coluna não existir no cabeçalho.
        """
        try:
            return self.campos.index(nome_coluna)
        except ValueError:
            raise ValueError(f"A coluna '{nome_coluna}' não existe no cabeçalho.")

    def __repr__(self) -> str:
        return (
            f"Cabecalho(campos={self.campos}, "
            f"delimitador='{self.delimitador}', "
            f"quebra_linha={repr(self.quebra_linha)})"
        )


class BaseICSV():
    """Classe base responsável pela leitura e armazenamento de dados CSV.

    Suporta carregamento a partir de arquivo ou de string em memória.
    Detecta automaticamente o delimitador, a presença de cabeçalho e a
    quebra de linha quando esses parâmetros não são informados.

    Normalmente não é instanciada diretamente — utilize `ICSV`, que herda
    desta classe e adiciona as operações de manipulação e análise.
    """

    def __init__(
        self,
        caminho: str = "",
        texto: str = "",
        encoding: str = "utf-8",
        possui_cabecalho: bool | None = None,
        delimitador: str | None = None,
        quebra_linha: str | None = None,
        tratamento_linhas_irregulares: str = "avisar",
        modo_leitura: str = "eager",
    ) -> None:
        """Inicializa e carrega os dados CSV.

        Ao menos um de `caminho` ou `texto` deve ser fornecido para que
        dados sejam carregados. Os parâmetros `delimitador`, `possui_cabecalho`
        e `quebra_linha` são detectados via `csv.Sniffer` quando não informados
        (i.e., quando deixados como `None`).

        Args:
            caminho: Caminho para o arquivo CSV a ser lido.
            texto: String com o conteúdo CSV a ser processado em memória.
            encoding: Codificação do arquivo (padrão: 'utf-8').
            possui_cabecalho: True se a primeira linha contém nomes de
                              colunas; False se não há cabeçalho; None para
                              detecção automática.
            delimitador: Caractere separador de campos (ex.: ',' ou ';').
                         None para detecção automática.
            quebra_linha: Sequência de fim de linha ('\\n', '\\r\\n', '\\r').
                          None para detecção automática.
            tratamento_linhas_irregulares: Política aplicada quando uma
                          linha tiver quantidade de colunas diferente do
                          esperado. Aceita `'avisar'` (padrão) ou
                          `'preencher'`.
            modo_leitura: Estratégia de leitura dos dados. Aceita
                          `'eager'` (padrão, carrega tudo em memória) ou
                          `'stream'` (lê linha a linha sob demanda).
        """
        self.__caminho: str = ""
        self.__texto: str = ""
        self.__delimitador: str | None = delimitador
        self.__possui_cabecalho: bool | None = possui_cabecalho
        self.__quebra_linha: str | None = quebra_linha
        self.__encoding: str = encoding
        self.__tratamento_linhas_irregulares: str = self.__validar_tratamento_linhas_irregulares(
            tratamento_linhas_irregulares
        )
        self.__modo_leitura: str = self.__validar_modo_leitura(modo_leitura)

        self.__cabecalho: Cabecalho | None = None
        self.__linhas: list[Linha] = []

        self.__ler_dados(caminho, texto, encoding)

    # --- Properties ---

    @property
    def caminho(self) -> str:
        """Caminho do arquivo de origem (vazio se carregado de texto)."""
        return self.__caminho

    @property
    def texto(self) -> str:
        """Texto CSV de origem (vazio se carregado de arquivo)."""
        return self.__texto

    @property
    def delimitador(self) -> str | None:
        """Caractere separador de campos detectado ou informado."""
        return self.__delimitador

    @property
    def possui_cabecalho(self) -> bool | None:
        """True se o arquivo possui uma linha de cabeçalho com nomes de colunas."""
        return self.__possui_cabecalho

    @property
    def quebra_linha(self) -> str | None:
        """Sequência de fim de linha detectada ou informada."""
        return self.__quebra_linha

    @property
    def encoding(self) -> str:
        """Codificação usada na leitura e escrita do arquivo."""
        return self.__encoding

    @property
    def tratamento_linhas_irregulares(self) -> str:
        """Política usada ao encontrar linhas com tamanho diferente do esperado."""
        return self.__tratamento_linhas_irregulares

    @property
    def modo_leitura(self) -> str:
        """Modo de leitura ativo: `'eager'` ou `'stream'`."""
        return self.__modo_leitura

    @property
    def cabecalho(self) -> Cabecalho | None:
        """Objeto `Cabecalho` com os nomes das colunas, ou None se ausente."""
        return self.__cabecalho

    @property
    def linhas(self) -> list[Linha]:
        """Lista de objetos `Linha` com os dados do arquivo (excluindo o cabeçalho)."""
        self._garantir_dados_materializados("Acesso a linhas")
        return self.__linhas

    # --- Setters ---

    @cabecalho.setter
    def cabecalho(self, novo_cabecalho: Cabecalho | None) -> None:
        self.__cabecalho = novo_cabecalho

    @caminho.setter
    def caminho(self, novo_caminho: str) -> None:
        self.__caminho = novo_caminho

    @texto.setter
    def texto(self, novo_texto: str) -> None:
        self.__texto = novo_texto

    @delimitador.setter
    def delimitador(self, novo_delimitador: str) -> None:
        self.__delimitador = novo_delimitador

    @possui_cabecalho.setter
    def possui_cabecalho(self, valor: bool | None) -> None:
        self.__possui_cabecalho = valor

    @quebra_linha.setter
    def quebra_linha(self, nova_quebra: str) -> None:
        self.__quebra_linha = nova_quebra

    @encoding.setter
    def encoding(self, novo_encoding: str) -> None:
        self.__encoding = novo_encoding

    @tratamento_linhas_irregulares.setter
    def tratamento_linhas_irregulares(self, novo_tratamento: str) -> None:
        self.__tratamento_linhas_irregulares = self.__validar_tratamento_linhas_irregulares(
            novo_tratamento
        )

    @staticmethod
    def __validar_modo_leitura(modo_leitura: str) -> str:
        """Valida e normaliza o modo de leitura."""
        modo_normalizado = modo_leitura.strip().lower()
        if modo_normalizado not in {"eager", "stream"}:
            raise ValueError("modo_leitura deve ser 'eager' ou 'stream'.")
        return modo_normalizado

    # --- Métodos internos ---

    @staticmethod
    def __validar_tratamento_linhas_irregulares(tratamento: str) -> str:
        """Valida e normaliza a política de linhas irregulares."""
        tratamento_normalizado = tratamento.strip().lower()
        if tratamento_normalizado not in {"avisar", "preencher"}:
            raise ValueError(
                "tratamento_linhas_irregulares deve ser 'avisar' ou 'preencher'."
            )
        return tratamento_normalizado

    @staticmethod
    def __normalizar_amostra_para_sniffer(amostra: str) -> str:
        """Remove última linha incompleta para reduzir falsos erros do Sniffer."""
        if not amostra:
            return amostra
        if amostra.endswith(("\n", "\r")):
            return amostra
        ultima_quebra = max(amostra.rfind("\n"), amostra.rfind("\r"))
        if ultima_quebra == -1:
            return amostra
        return amostra[:ultima_quebra + 1]

    @staticmethod
    def __detectar_delimitador_fallback(amostra: str) -> str:
        """Detecta delimitador por heurística simples quando o Sniffer falha."""
        candidatos = [",", ";", "\t", "|", ":"]
        linhas = [linha for linha in amostra.splitlines() if linha.strip()]
        if not linhas:
            return ","

        melhor_delimitador = ","
        melhor_score: tuple[int, int, int] = (-1, -1, -1)

        for delimitador in candidatos:
            contagens = [linha.count(delimitador) for linha in linhas]
            positivas = [valor for valor in contagens if valor > 0]
            if not positivas:
                continue

            linhas_com_delimitador = len(positivas)
            variacao = max(positivas) - min(positivas)
            total = sum(positivas)
            score = (linhas_com_delimitador, -variacao, total)

            if score > melhor_score:
                melhor_score = score
                melhor_delimitador = delimitador

        return melhor_delimitador

    @staticmethod
    def __encodings_tentativa(encoding_preferido: str) -> list[str]:
        """Retorna encodings a tentar, priorizando o informado pelo usuário."""
        candidatos = [encoding_preferido]

        encoding_normalizado = encoding_preferido.lower().replace("_", "-")
        if encoding_normalizado == "utf-8":
            candidatos.extend(["utf-8-sig", "cp1252", "iso-8859-1"])
        elif encoding_normalizado == "utf-8-sig":
            candidatos.extend(["utf-8", "cp1252", "iso-8859-1"])
        elif encoding_normalizado == "cp1252":
            candidatos.extend(["iso-8859-1", "utf-8", "utf-8-sig"])
        elif encoding_normalizado in ("iso-8859-1", "latin-1", "latin1"):
            candidatos.extend(["cp1252", "utf-8", "utf-8-sig"])
        else:
            candidatos.extend(["utf-8", "utf-8-sig", "cp1252", "iso-8859-1"])

        encodings_unicos: list[str] = []
        vistos: set[str] = set()
        for candidato in candidatos:
            chave = candidato.lower().replace("_", "-")
            if chave not in vistos:
                vistos.add(chave)
                encodings_unicos.append(candidato)
        return encodings_unicos

    def __ler_texto_arquivo_com_fallback(self, caminho: str, encoding_preferido: str) -> tuple[str, str]:
        """Lê o arquivo em binário e tenta decodificar com fallback de encoding."""
        with open(caminho, 'rb') as file:
            conteudo_bytes = file.read()

        ultimo_erro: UnicodeDecodeError | None = None
        for encoding_tentado in self.__encodings_tentativa(encoding_preferido):
            try:
                return conteudo_bytes.decode(encoding_tentado), encoding_tentado
            except UnicodeDecodeError as erro:
                ultimo_erro = erro

        if ultimo_erro is not None:
            raise ultimo_erro

        return conteudo_bytes.decode(encoding_preferido), encoding_preferido

    def __detectar_encoding_e_amostra_arquivo(self, caminho: str, encoding_preferido: str) -> tuple[str, str]:
        """Detecta encoding lendo apenas uma amostra, sem carregar o arquivo inteiro."""
        ultimo_erro: UnicodeDecodeError | None = None
        for encoding_tentado in self.__encodings_tentativa(encoding_preferido):
            try:
                with open(caminho, 'r', encoding=encoding_tentado, newline='') as file:
                    return encoding_tentado, file.read(65536)
            except UnicodeDecodeError as erro:
                ultimo_erro = erro

        if ultimo_erro is not None:
            raise ultimo_erro

        with open(caminho, 'r', encoding=encoding_preferido, newline='') as file:
            return encoding_preferido, file.read(65536)

    @staticmethod
    def __normalizar_tamanho_linha(linha: list[str], colunas_esperadas: int) -> list[str]:
        """Ajusta a linha para o número esperado de colunas."""
        if len(linha) < colunas_esperadas:
            return linha + [""] * (colunas_esperadas - len(linha))
        return linha[:colunas_esperadas]

    def __tratar_linha_irregular(
        self,
        linha: list[str],
        numero_linha: int,
        colunas_esperadas: int,
    ) -> list[str]:
        """Aplica a política configurada para linhas com tamanho irregular."""
        if len(linha) == colunas_esperadas:
            return linha

        mensagem = (
            f"Linha {numero_linha} possui {len(linha)} colunas; esperado: "
            f"{colunas_esperadas}."
        )

        if self.__tratamento_linhas_irregulares == "preencher":
            return self.__normalizar_tamanho_linha(linha, colunas_esperadas)

        warnings.warn(
            mensagem + " Use tratamento_linhas_irregulares='preencher' para normalizar no parse.",
            UserWarning,
            stacklevel=3,
        )
        return linha

    def __sonda(self, amostra: str) -> None:
        """Detecta automaticamente delimitador, cabeçalho e quebra de linha.

        Usa `csv.Sniffer` para analisar uma amostra do conteúdo. Só preenche
        os atributos que ainda estão como None — valores já informados pelo
        usuário não são sobrescritos.

        Args:
            amostra: Primeiros bytes ou caracteres do conteúdo CSV.
        """
        amostra = self.__normalizar_amostra_para_sniffer(amostra)
        sniffer = csv.Sniffer()
        if self.__delimitador is None:
            try:
                self.__delimitador = sniffer.sniff(amostra, delimiters=",;\t|:").delimiter
            except csv.Error:
                self.__delimitador = self.__detectar_delimitador_fallback(amostra)
        if self.__possui_cabecalho is None:
            try:
                self.__possui_cabecalho = sniffer.has_header(amostra)
            except csv.Error:
                self.__possui_cabecalho = False
        if self.__quebra_linha is None:
            quebra = detectar_quebra_de_linha_texto(amostra)
            self.__quebra_linha = quebra.value if quebra.value else "\n"

    def __gerar_mapa_colunas(self) -> dict[str, int]:
        """Gera o dicionário de mapeamento `{nome_coluna_lower: indice}`.

        Usado para injetar nas linhas a capacidade de acesso por nome de
        coluna via `__getattr__`. Retorna dicionário vazio se não há cabeçalho.

        Returns:
            Dicionário `{str: int}` ou `{}` se não houver cabeçalho.
        """
        if not self.cabecalho:
            return {}
        return {nome: i for i, nome in enumerate(self.cabecalho.campos)}

    def __iterar_linhas_parseadas(self, reader_local: csv.reader) -> Iterator[Linha]:
        """Converte um csv.reader em objetos Linha aplicando validações de parse."""
        colunas_esperadas: int | None = None
        if self.__cabecalho:
            colunas_esperadas = len(self.__cabecalho.campos)

        for numero_linha, linha in enumerate(reader_local, start=1):
            if self.__possui_cabecalho and numero_linha == 1:
                if not self.__cabecalho:
                    self.__cabecalho = Cabecalho(
                        campos=linha,
                        delimitador=self.__delimitador,
                        quebra_linha=self.__quebra_linha,
                    )
                    colunas_esperadas = len(self.__cabecalho.campos)
                continue

            if colunas_esperadas is None:
                colunas_esperadas = len(linha)
            else:
                linha = self.__tratar_linha_irregular(linha, numero_linha, colunas_esperadas)

            yield Linha(
                campos=linha,
                delimitador=self.__delimitador,
                quebra_linha=self.__quebra_linha,
                mapa_colunas=self.__gerar_mapa_colunas(),
            )

    def __iterar_linhas_stream(self) -> Iterator[Linha]:
        """Itera linhas sob demanda sem materializar todo o arquivo em memória."""
        if self.__caminho:
            with open(self.__caminho, 'r', encoding=self.__encoding, newline='') as file:
                reader = csv.reader(file, delimiter=self.__delimitador)
                yield from self.__iterar_linhas_parseadas(reader)
            return

        if self.__texto:
            reader = csv.reader(io.StringIO(self.__texto), delimiter=self.__delimitador)
            yield from self.__iterar_linhas_parseadas(reader)

    def __ler_dados(self, caminho: str, texto: str, encoding: str) -> None:
        """Lê e processa o conteúdo CSV de um arquivo ou string.

        Executa a sondagem automática dos metadados antes de iniciar a
        leitura linha a linha. A primeira linha é tratada como cabeçalho
        se `possui_cabecalho` for True.

        Args:
            caminho: Caminho do arquivo a ler (ignorado se vazio).
            texto: Conteúdo CSV em string (usado se `caminho` for vazio).
            encoding: Codificação do arquivo.
        """
        if caminho:
            self.__caminho = caminho
            if self.__modo_leitura == "stream":
                encoding_utilizado, amostra = self.__detectar_encoding_e_amostra_arquivo(caminho, encoding)
                self.__encoding = encoding_utilizado
                self.__sonda(amostra)

                if self.__possui_cabecalho:
                    with open(caminho, 'r', encoding=self.__encoding, newline='') as file:
                        reader = csv.reader(file, delimiter=self.__delimitador)
                        primeira_linha = next(reader, None)
                        if primeira_linha is not None:
                            self.__cabecalho = Cabecalho(
                                campos=primeira_linha,
                                delimitador=self.__delimitador,
                                quebra_linha=self.__quebra_linha,
                            )
            else:
                texto_arquivo, encoding_utilizado = self.__ler_texto_arquivo_com_fallback(caminho, encoding)
                self.__encoding = encoding_utilizado
                self.__sonda(texto_arquivo[:65536])
                reader = csv.reader(io.StringIO(texto_arquivo), delimiter=self.__delimitador)
                self.__linhas = list(self.__iterar_linhas_parseadas(reader))
        elif texto:
            self.__texto = texto
            self.__sonda(self.__texto[0:65536])
            if self.__modo_leitura == "stream":
                if self.__possui_cabecalho:
                    reader = csv.reader(io.StringIO(texto), delimiter=self.__delimitador)
                    primeira_linha = next(reader, None)
                    if primeira_linha is not None:
                        self.__cabecalho = Cabecalho(
                            campos=primeira_linha,
                            delimitador=self.__delimitador,
                            quebra_linha=self.__quebra_linha,
                        )
            else:
                reader = csv.reader(io.StringIO(texto), delimiter=self.__delimitador)
                self.__linhas = list(self.__iterar_linhas_parseadas(reader))

    # --- Interface pública ---

    def __iter__(self) -> Iterator[Linha]:
        """Itera sobre as linhas de dados (excluindo o cabeçalho)."""
        if self.__modo_leitura == "stream":
            yield from self.__iterar_linhas_stream()
            return

        for linha in self.__linhas:
            yield linha

    def __len__(self) -> int:
        """Retorna o número de linhas de dados (excluindo o cabeçalho)."""
        if self.__modo_leitura == "stream":
            return sum(1 for _ in self.__iterar_linhas_stream())
        return len(self.__linhas)

    def len_stream_estimate(
        self,
        amostra_linhas: int = 1000,
        retornar_intervalo: bool = False,
        confianca: float = 0.95,
    ) -> int | tuple[int, tuple[int, int]]:
        """Estima número de linhas de dados sem leitura completa do arquivo.

        Em `modo_leitura='stream'`, evita varrer todo o arquivo em disco,
        estimando o total com base no tamanho do arquivo e no tamanho médio
        das primeiras linhas da amostra.

        Para objetos em modo eager, retorna `len(self)` (valor exato).

        Args:
            amostra_linhas: Quantidade de linhas usadas na amostra.
            retornar_intervalo: Se True, retorna também faixa aproximada
                `(limite_inferior, limite_superior)` baseada no nível de confiança.
            confianca: Nível de confiança para os intervalos (0.0 a 1.0).
                Ex.: 0.90 (90%), 0.95 (95%), 0.99 (99%).
                Padrão: 0.95 (95%).

        Returns:
            - `int` com estimativa (ou valor exato fora do stream), quando
              `retornar_intervalo=False`.
            - `tuple[int, tuple[int, int]]` quando `retornar_intervalo=True`.

        Raises:
            ValueError: Se amostra_linhas <= 0 ou confianca não está em (0, 1).

        ⚠️ Limitação — Campos Multilinhas:
            A estimativa assume que cada `\n` representa uma linha csv.
            Se o arquivo contiver campos entre aspas com quebras de linha internas
            (ex: descrições longas em Moodle, comentários com múltiplas linhas),
            a contagem será imprecisa: o desvio padrão de tamanhos aumenta,
            causando intervalos maiores e estimativa menos confiável.
            Esta limitação raramente afeta logs gerados por máquinas (alvo comum).
            Para arquivos com muitos campos multilinhas, prefira `len()` (varredura completa).
        """
        if amostra_linhas <= 0:
            raise ValueError("amostra_linhas deve ser maior que zero.")
        if not (0.0 < confianca < 1.0):
            raise ValueError("confianca deve estar entre 0.0 e 1.0 (exclusivo).")

        z_scores = {
            0.80: 1.282,
            0.85: 1.440,
            0.90: 1.645,
            0.95: 1.960,
            0.99: 2.576,
        }

        def obter_z_score(conf: float) -> float:
            if conf in z_scores:
                return z_scores[conf]
            chaves = sorted(z_scores.keys())
            for i in range(len(chaves) - 1):
                if chaves[i] < conf < chaves[i + 1]:
                    t1, t2 = chaves[i], chaves[i + 1]
                    z1, z2 = z_scores[t1], z_scores[t2]
                    return z1 + (z2 - z1) * (conf - t1) / (t2 - t1)
            if conf < chaves[0]:
                return z_scores[chaves[0]]
            return z_scores[chaves[-1]]

        def montar_retorno(estimativa: int, margem_relativa: float = 0.0) -> int | tuple[int, tuple[int, int]]:
            estimativa_normalizada = max(0, estimativa)
            if not retornar_intervalo:
                return estimativa_normalizada

            margem_relativa_normalizada = max(0.0, margem_relativa)
            margem_abs = int(estimativa_normalizada * margem_relativa_normalizada)
            limite_inferior = max(0, estimativa_normalizada - margem_abs)
            limite_superior = estimativa_normalizada + margem_abs
            return estimativa_normalizada, (limite_inferior, limite_superior)

        if self.__modo_leitura != "stream":
            return montar_retorno(len(self), 0.0)

        if self.__texto:
            total_linhas = len(self.__texto.splitlines())
            if self.__possui_cabecalho and total_linhas > 0:
                total_linhas -= 1
            return montar_retorno(total_linhas, 0.0)

        if not self.__caminho:
            return montar_retorno(0, 0.0)

        total_bytes_arquivo = os.path.getsize(self.__caminho)
        linhas_amostra: list[str] = []
        limite_leitura = amostra_linhas + (1 if self.__possui_cabecalho else 0)

        with open(self.__caminho, 'r', encoding=self.__encoding, newline='') as file:
            for _ in range(limite_leitura):
                linha = file.readline()
                if not linha:
                    break
                linhas_amostra.append(linha)

        if not linhas_amostra:
            return montar_retorno(0, 0.0)

        tamanhos_linhas = [
            len(linha.encode(self.__encoding, errors='replace'))
            for linha in linhas_amostra
        ]
        media_bytes_por_linha = sum(tamanhos_linhas) / len(tamanhos_linhas)
        if media_bytes_por_linha <= 0:
            return montar_retorno(0, 0.0)

        estimativa_total_linhas = int(total_bytes_arquivo / media_bytes_por_linha)
        if self.__possui_cabecalho and estimativa_total_linhas > 0:
            estimativa_total_linhas -= 1

        desvio_padrao = statistics.pstdev(tamanhos_linhas) if len(tamanhos_linhas) > 1 else 0.0
        z_score = obter_z_score(confianca)
        margem_relativa = min(0.5, z_score * desvio_padrao / media_bytes_por_linha) if media_bytes_por_linha > 0 else 0.0

        return montar_retorno(estimativa_total_linhas, margem_relativa)

    def __str__(self) -> str:
        """Retorna uma representação resumida do objeto para exibição."""
        return (
            f"icsv(caminho='{self.__caminho}', "
            f"possui_cabecalho={self.__possui_cabecalho}, "
            f"delimitador='{self.__delimitador}', "
            f"quebra_linha='{self.__quebra_linha}', "
            f"modo_leitura='{self.__modo_leitura}')"
        )

    def to_list_of_dicts(self) -> list[dict[str, str]]:
        """Converte os dados para uma lista de dicionários.

        Se houver cabeçalho, as chaves são os nomes das colunas.
        Sem cabeçalho, as chaves são geradas como `col_0`, `col_1`, etc.

        Returns:
            Lista de dicionários, um por linha de dados.

        Exemplo:
            [{"nome": "Ana", "idade": "30"}, {"nome": "Bruno", "idade": "25"}]
        """
        if not self.__cabecalho:
            return [
                {"col_" + str(i): campo for i, campo in enumerate(linha.campos)}
                for linha in self
            ]
        colunas = self.__cabecalho.campos
        return [
            {colunas[i]: linha.obter_campo(i) for i in range(len(colunas))}
            for linha in self
        ]

    def to_json(self) -> str:
        """Serializa o objeto completo para JSON, incluindo metadados e dados.

        Returns:
            String JSON com indentação de 4 espaços.
        """
        data = {
            "caminho": self.__caminho,
            "texto": self.__texto,
            "delimitador": self.__delimitador,
            "possui_cabecalho": self.__possui_cabecalho,
            "quebra_linha": self.__quebra_linha,
            "modo_leitura": self.__modo_leitura,
            "cabecalho": self.__cabecalho.to_dict() if self.__cabecalho else None,
            "linhas": [linha.to_dict() for linha in self],
        }
        return json.dumps(data, ensure_ascii=False, indent=4)

    def info(self) -> str:
        """Retorna um resumo textual das propriedades do arquivo CSV.

        Inclui caminho, delimitador, quebra de linha, presença de cabeçalho,
        número de linhas, número de colunas e nomes das colunas.

        Returns:
            String multi-linha com as informações formatadas.
        """
        if self.__modo_leitura == "stream":
            num_linhas: str | int = "Desconhecido (modo stream)"
            num_colunas: str | int = (
                len(self.__cabecalho.campos)
                if self.__cabecalho
                else "N/A (modo stream)"
            )
        else:
            num_linhas = len(self)
            num_colunas = (
                len(self.__cabecalho.campos)
                if self.__cabecalho
                else (len(self.__linhas[0].campos) if self.__linhas else 0)
            )
        ret = f"Arquivo: {self.__caminho or 'Texto em memória'}\n"
        ret += f"Delimitador: '{self.__delimitador}'\n"
        ret += f"Quebra de linha: {repr(self.__quebra_linha)}\n"
        ret += f"Possui cabeçalho: {self.__possui_cabecalho}\n"
        ret += f"Número de linhas: {num_linhas}\n"
        ret += f"Número de colunas: {num_colunas}\n"
        ret += "Colunas: " + (", ".join(self.__cabecalho.campos) if self.__cabecalho else "N/A") + "\n"
        return ret

    def info_json(self) -> str:
        """Retorna um resumo das propriedades do arquivo CSV em formato JSON.

        Diferente de `to_json()`, não inclui os dados das linhas — apenas
        os metadados do arquivo.

        Returns:
            String JSON com indentação de 4 espaços.
        """
        if self.__modo_leitura == "stream":
            numero_linhas: int | None = None
            numero_colunas: int | None = len(self.__cabecalho.campos) if self.__cabecalho else None
        else:
            numero_linhas = len(self)
            numero_colunas = (
                len(self.__cabecalho.campos)
                if self.__cabecalho
                else (len(self.__linhas[0].campos) if self.__linhas else 0)
            )

        info_dict = {
            "arquivo": self.__caminho or 'Texto em memória',
            "delimitador": self.__delimitador,
            "quebra_linha": self.__quebra_linha,
            "possui_cabecalho": self.__possui_cabecalho,
            "modo_leitura": self.__modo_leitura,
            "numero_linhas": numero_linhas,
            "numero_colunas": numero_colunas,
            "colunas": self.__cabecalho.campos if self.__cabecalho else [],
        }
        return json.dumps(info_dict, ensure_ascii=False, indent=4)

    def preview(self) -> str:
        """Retorna o cabeçalho e as primeiras 5 linhas formatadas como CSV.

        Útil para inspecionar rapidamente o conteúdo sem iterar o arquivo
        inteiro. Para arquivos grandes, evite usar `str(dados)` ou imprimir
        todas as linhas — prefira este método.

        Returns:
            String com o cabeçalho (se existir) seguido de até 5 linhas de dados.
        """
        ret = ''
        if self.cabecalho:
            ret += str(self.cabecalho) + self.quebra_linha
        for linha in itertools.islice(self, 5):
            ret += str(linha) + self.quebra_linha
        return ret

    def _garantir_dados_materializados(self, operacao: str) -> None:
        """Garante que a operação rode apenas em modo eager."""
        if self.__modo_leitura == "stream":
            raise RuntimeError(
                f"{operacao} não está disponível em modo_leitura='stream'. "
                "Recarregue com modo_leitura='eager' para operar em memória."
            )


class ICSV(BaseICSV):
    """Classe principal para manipulação e análise de arquivos CSV.

    Estende `BaseICSV` com operações de filtragem, ordenação, transformação,
    combinação e exportação. Todas as operações que retornam um subconjunto
    dos dados criam um novo objeto `ICSV` — as operações in-place são
    explicitamente identificadas na documentação de cada método.

    Exemplo:
        dados = ICSV('colaboradores.csv')

        # Encadeamento de operações (cada etapa retorna novo ICSV)
        resultado = (dados
            .filtrar_por_coluna('departamento', 'TI')
            .filtrar_por_coluna('status', 'ativo')
            .selecionar_colunas(['nome', 'cargo', 'salario']))

        resultado.salvar_como('ti_ativos.csv')
    """

    def __init__(
        self,
        caminho: str = "",
        texto: str = "",
        encoding: str = "utf-8",
        possui_cabecalho: bool | None = None,
        delimitador: str | None = None,
        quebra_linha: str | None = None,
        tratamento_linhas_irregulares: str = "avisar",
        modo_leitura: str = "eager",
    ) -> None:
        """Inicializa o ICSV a partir de arquivo, string ou vazio (para construção programática).

        Args:
            caminho: Caminho para o arquivo CSV a ser lido.
            texto: String com o conteúdo CSV a ser processado em memória.
            encoding: Codificação do arquivo (padrão: 'utf-8').
            possui_cabecalho: True/False para forçar; None para detecção automática.
            delimitador: Caractere separador; None para detecção automática.
            quebra_linha: Sequência de fim de linha; None para detecção automática.
            tratamento_linhas_irregulares: Política para linhas com quantidade
                irregular de colunas. Aceita `'avisar'` (padrão) ou `'preencher'`.
            modo_leitura: `'eager'` (padrão) para carregar tudo em memória,
                ou `'stream'` para leitura sob demanda.
        """
        super().__init__(
            caminho,
            texto,
            encoding,
            possui_cabecalho,
            delimitador,
            quebra_linha,
            tratamento_linhas_irregulares,
            modo_leitura,
        )

    # --- Métodos internos ---

    def _novo_com_linhas(self, linhas: Iterable[Linha]) -> ICSV:
        """Cria um novo ICSV com o mesmo cabeçalho e delimitador, mas com novas linhas.

        Método auxiliar utilizado internamente por `filtrar_por_coluna`,
        `head`, `tail`, `deduplicar` e outros. Garante que o `mapa_colunas`
        de cada linha seja corretamente atualizado no novo objeto.

        Args:
            linhas: Iterável de objetos `Linha` a incluir no resultado.

        Returns:
            Novo objeto `ICSV` com cópia das linhas fornecidas.
        """
        resultado = ICSV(delimitador=self.delimitador, quebra_linha=self.quebra_linha)
        if self.cabecalho:
            resultado.cabecalho = Cabecalho(
                campos=self.cabecalho.campos.copy(),
                delimitador=self.delimitador,
            )
            resultado.possui_cabecalho = True
            mapa = {nome: i for i, nome in enumerate(resultado.cabecalho.campos)}
            for linha in linhas:
                nova = Linha(campos=linha.campos.copy(), delimitador=self.delimitador)
                nova.definir_mapa_colunas(mapa)
                resultado.linhas.append(nova)
        else:
            resultado.possui_cabecalho = False
            for linha in linhas:
                nova = Linha(campos=linha.campos.copy(), delimitador=self.delimitador)
                resultado.linhas.append(nova)
        return resultado

    def _novo_com_linhas_atuais(self) -> ICSV:
        """Cria um novo ICSV carregando dados materializados (eager-only)."""
        self._garantir_dados_materializados("_novo_com_linhas_atuais")
        return self._novo_com_linhas(self.linhas)

    # --- Acesso indexado ---

    def __getitem__(self, indice: int | slice) -> Linha | ICSV:
        """Acessa uma linha por índice inteiro ou retorna um novo ICSV via slice.

        Args:
            indice: Inteiro para obter uma `Linha` específica, ou `slice`
                    para obter um subconjunto como novo `ICSV`.

        Returns:
            Objeto `Linha` se `indice` for int; novo `ICSV` se for slice.

        Raises:
            TypeError: Se `indice` não for int nem slice.
            IndexError: Se o índice inteiro estiver fora do intervalo.

        Exemplo:
            dados[0]      # primeira linha
            dados[-1]     # última linha
            dados[0:10]   # novo ICSV com as 10 primeiras linhas
        """
        if isinstance(indice, int):
            if self.modo_leitura == "stream":
                raise RuntimeError(
                    "Acesso por índice inteiro não está disponível em modo_leitura='stream'. "
                    "Use iteração (for linha in dados) ou head/tail para extrair subsets."
                )
            return self.linhas[indice]
        if isinstance(indice, slice):
            if self.modo_leitura == "stream":
                return self.head(slice.stop if slice.stop else 0) if slice.stop else self._novo_com_linhas([])
            return self._novo_com_linhas(self.linhas[indice])
        raise TypeError(f"Índice deve ser int ou slice, não {type(indice).__name__}.")

    # --- Ordenação ---

    def order_by_field_name(
        self,
        nome_coluna: str,
        reverse: bool = False,
        cast_type: Callable[[str], Any] = str,
    ) -> None:
        """Ordena as linhas in-place pelo valor de uma coluna identificada pelo nome.

        Args:
            nome_coluna: Nome exato da coluna a usar como chave de ordenação.
            reverse: Se True, ordena em ordem decrescente (padrão: False).
            cast_type: Callable que converte o valor de string para o tipo
                       desejado antes da comparação. Use `int` ou `float` para
                       ordenação numérica (padrão: `str`).

        Raises:
            ValueError: Se não houver cabeçalho ou a coluna não existir.

        Exemplo:
            dados.order_by_field_name('salario', reverse=True, cast_type=int)
        """
        self._garantir_dados_materializados("order_by_field_name")
        if not self.cabecalho:
            raise ValueError("Não é possível ordenar sem um cabeçalho definido.")
        indice_coluna = self.cabecalho.obter_indice_da_coluna(nome_coluna)
        self.order_by_field_index(indice_coluna, reverse, cast_type)

    def order_by_field_index(
        self,
        indice_coluna: int,
        reverse: bool = False,
        cast_type: Callable[[str], Any] = str,
    ) -> None:
        """Ordena as linhas in-place pelo valor de uma coluna identificada pelo índice.

        Args:
            indice_coluna: Índice zero-based da coluna a usar como chave.
            reverse: Se True, ordena em ordem decrescente (padrão: False).
            cast_type: Callable que converte o valor antes da comparação (padrão: `str`).

        Raises:
            IndexError: Se `indice_coluna` estiver fora do intervalo.
        """
        self._garantir_dados_materializados("order_by_field_index")
        if not self.linhas:
            return
        total_campos = self.linhas[0].numero_de_campos()
        if indice_coluna < 0 or indice_coluna >= total_campos:
            raise IndexError("Índice de coluna fora do intervalo.")
        self.linhas.sort(
            key=lambda linha: cast_type(linha.obter_campo(indice_coluna)),
            reverse=reverse,
        )

    # --- Persistência ---

    def salvar(self) -> None:
        """Salva os dados no arquivo de origem (sobrescreve).

        Requer que `caminho` esteja definido. Usa a codificação e o
        delimitador do objeto.

        Raises:
            FileNotFoundError: Se o diretório de destino não existir.
        """
        self.salvar_como(self.caminho)

    def salvar_como(self, novo_caminho: str) -> None:
        """Salva os dados em um novo arquivo CSV.

        Escreve o cabeçalho (se presente) seguido das linhas de dados,
        usando o delimitador e a quebra de linha do objeto.

        Args:
            novo_caminho: Caminho do arquivo de destino.

        Raises:
            FileNotFoundError: Se o diretório de destino não existir.
        """
        with open(novo_caminho, 'w', encoding=self.encoding, newline='') as file:
            escritor = csv.writer(file, delimiter=self.delimitador, lineterminator=self.quebra_linha)
            if self.possui_cabecalho and self.cabecalho:
                escritor.writerow(self.cabecalho.campos)
            for linha in self:
                escritor.writerow(linha.campos)

    # --- Manipulação de linhas ---

    def adicionar_linha(self, linha: Linha) -> None:
        """Adiciona uma linha ao final do arquivo (in-place).

        Injeta automaticamente o `mapa_colunas` na linha, habilitando
        o acesso por nome de coluna via atributo.

        Args:
            linha: Objeto `Linha` a adicionar.

        Raises:
            ValueError: Se o número de campos da linha for diferente do
                        número de colunas do cabeçalho.
        """
        self._garantir_dados_materializados("adicionar_linha")
        if self.cabecalho and linha.numero_de_campos() != self.cabecalho.numero_de_campos():
            raise ValueError("A linha não possui a mesma quantidade de colunas do cabeçalho.")
        if self.cabecalho:
            mapa = {nome: i for i, nome in enumerate(self.cabecalho.campos)}
            linha.definir_mapa_colunas(mapa)
        self.linhas.append(linha)

    def remover_linha(self, index: int) -> None:
        """Remove a linha no índice especificado (in-place).

        Args:
            index: Índice zero-based da linha a remover.

        Raises:
            IndexError: Se `index` estiver fora do intervalo válido.
        """
        self._garantir_dados_materializados("remover_linha")
        if 0 <= index < len(self.linhas):
            self.linhas.pop(index)
        else:
            raise IndexError("Índice de linha inválido.")

    # --- Manipulação de colunas ---

    def adicionar_coluna(self, nome_coluna: str, valor_padrao: str = "") -> None:
        """Adiciona uma nova coluna no final de todas as linhas (in-place).

        O cabeçalho e o `mapa_colunas` de cada linha são atualizados
        automaticamente para refletir a nova estrutura.

        Args:
            nome_coluna: Nome da nova coluna.
            valor_padrao: Valor inicial para a coluna em todas as linhas
                          existentes (padrão: string vazia).

        Raises:
            ValueError: Se não houver cabeçalho definido.
        """
        if not self.cabecalho:
            raise ValueError("Não é possível adicionar coluna sem um cabeçalho.")
        self.cabecalho.adicionar_campo(nome_coluna)
        novo_mapa = {nome: i for i, nome in enumerate(self.cabecalho.campos)}
        for linha in self.linhas:
            linha.adicionar_campo(valor_padrao)
            linha.definir_mapa_colunas(novo_mapa)

    def remover_coluna(self, nome_coluna: str) -> None:
        """Remove uma coluna do cabeçalho e de todas as linhas (in-place).

        O `mapa_colunas` de cada linha é recalculado após a remoção.

        Args:
            nome_coluna: Nome exato da coluna a remover.

        Raises:
            ValueError: Se a coluna não existir no cabeçalho.
        """
        self._garantir_dados_materializados("remover_coluna")
        if not self.cabecalho:
            return
        indice = self.cabecalho.obter_indice_da_coluna(nome_coluna)
        self.cabecalho.remover_campo_por_indice(indice)
        novo_mapa = {nome: i for i, nome in enumerate(self.cabecalho.campos)}
        for linha in self.linhas:
            linha.remover_campo_por_indice(indice)
            linha.definir_mapa_colunas(novo_mapa)

    def atualizar_nome_coluna(self, nome_antigo: str, nome_novo: str) -> None:
        """Renomeia uma coluna no cabeçalho (in-place).

        O `mapa_colunas` de todas as linhas é atualizado para refletir
        o novo nome, mantendo o acesso por atributo funcionando.

        Args:
            nome_antigo: Nome atual da coluna.
            nome_novo: Novo nome a atribuir.

        Raises:
            ValueError: Se `nome_antigo` não existir no cabeçalho.
        """
        self._garantir_dados_materializados("atualizar_nome_coluna")
        if not self.cabecalho:
            return
        indice = self.cabecalho.obter_indice_da_coluna(nome_antigo)
        self.cabecalho.atualizar_campo(indice, nome_novo)
        novo_mapa = {nome: i for i, nome in enumerate(self.cabecalho.campos)}
        for linha in self.linhas:
            linha.definir_mapa_colunas(novo_mapa)

    def modificar_valores(
        self,
        coluna: str,
        funcao_modificadora: Callable[[str], Any],
        coluna_parametro: str | None = None,
    ) -> None:
        """Aplica uma função para transformar os valores de uma coluna (in-place).

        Por padrão, a função recebe o valor atual da própria coluna alvo.
        Se `coluna_parametro` for informado, a função recebe o valor dessa
        outra coluna, permitindo calcular o novo valor com base em dados externos.

        Args:
            coluna: Nome da coluna cujos valores serão substituídos.
            funcao_modificadora: Callable que recebe uma string e retorna
                                 o novo valor (também convertido para string).
            coluna_parametro: Nome de outra coluna a usar como entrada da
                              função. Se None, usa a própria `coluna` como entrada.

        Raises:
            ValueError: Se não houver cabeçalho ou a coluna não existir.

        Exemplo:
            # Reajuste de 10% no salário
            dados.modificar_valores('salario', lambda s: str(int(s) * 1.1))

            # Calcular bônus com base no salário
            dados.modificar_valores('bonus', lambda s: str(int(s) * 0.05), coluna_parametro='salario')
        """
        self._garantir_dados_materializados("modificar_valores")
        if not self.cabecalho:
            raise ValueError("Não é possível modificar valores sem um cabeçalho definido.")
        idx_alvo = self.cabecalho.obter_indice_da_coluna(coluna)
        idx_parametro = self.cabecalho.obter_indice_da_coluna(coluna_parametro) if coluna_parametro else idx_alvo
        for linha in self.linhas:
            valor_entrada = linha.obter_campo(idx_parametro)
            novo_valor = funcao_modificadora(valor_entrada)
            linha.atualizar_campo(idx_alvo, str(novo_valor).strip())

    # --- Subconjuntos ---

    def head(self, n: int = 5) -> ICSV:
        """Retorna um novo ICSV com as primeiras n linhas.

        Args:
            n: Número de linhas a retornar (padrão: 5).

        Returns:
            Novo `ICSV` com até `n` linhas.
        """
        linhas = list(itertools.islice(self, n))
        return self._novo_com_linhas(linhas)

    def tail(self, n: int = 5) -> ICSV:
        """Retorna um novo ICSV com as últimas n linhas.

        Args:
            n: Número de linhas a retornar (padrão: 5).

        Returns:
            Novo `ICSV` com até `n` linhas.
        """
        if self.modo_leitura == "stream":
            if n >= 0:
                ultimas = deque(self, maxlen=n)
                return self._novo_com_linhas(ultimas)

            all_linhas = list(self)
            return self._novo_com_linhas(all_linhas[-n:])
        return self._novo_com_linhas(self.linhas[-n:])

    def selecionar_colunas(self, nomes_colunas: list[str]) -> ICSV:
        """Retorna um novo ICSV contendo apenas as colunas indicadas (projeção).

        A ordem das colunas no resultado segue a ordem de `nomes_colunas`.

        Args:
            nomes_colunas: Lista com os nomes das colunas a manter.

        Returns:
            Novo `ICSV` com o subconjunto de colunas.

        Raises:
            ValueError: Se não houver cabeçalho ou alguma coluna não existir.

        Exemplo:
            resumo = dados.selecionar_colunas(['nome', 'cpf', 'status'])
        """
        if not self.cabecalho:
            raise ValueError("Não é possível selecionar colunas sem um cabeçalho definido.")
        indices = [self.cabecalho.obter_indice_da_coluna(nome) for nome in nomes_colunas]
        resultado = ICSV(delimitador=self.delimitador, quebra_linha=self.quebra_linha)
        resultado.cabecalho = Cabecalho(campos=list(nomes_colunas), delimitador=self.delimitador)
        resultado.possui_cabecalho = True
        mapa = {nome: i for i, nome in enumerate(nomes_colunas)}
        for linha in self:
            campos = [linha.obter_campo(i) for i in indices]
            nova = Linha(campos=campos, delimitador=self.delimitador)
            nova.definir_mapa_colunas(mapa)
            resultado.linhas.append(nova)
        return resultado

    # --- Combinação ---

    def concatenar(self, outro: ICSV) -> ICSV:
        """Retorna um novo ICSV com as linhas de ambos os arquivos empilhadas.

        Os cabeçalhos precisam ser idênticos (mesmos nomes na mesma ordem).
        Útil para unir arquivos com a mesma estrutura, como exportações mensais.

        Args:
            outro: Objeto `ICSV` cujas linhas serão adicionadas ao final.

        Returns:
            Novo `ICSV` com as linhas de `self` seguidas pelas de `outro`.

        Raises:
            TypeError: Se `outro` não for uma instância de `ICSV`.
            ValueError: Se os cabeçalhos forem diferentes.
        """
        if not isinstance(outro, ICSV):
            raise TypeError("Só é possível concatenar com outro ICSV.")
        if self.cabecalho and outro.cabecalho and self.cabecalho.campos != outro.cabecalho.campos:
            raise ValueError("Os cabeçalhos precisam ser idênticos para concatenar.")
        resultado = self._novo_com_linhas(list(self))
        mapa = (
            {nome: i for i, nome in enumerate(resultado.cabecalho.campos)}
            if resultado.cabecalho else {}
        )
        for linha in outro:
            nova = Linha(campos=linha.campos.copy(), delimitador=self.delimitador)
            nova.definir_mapa_colunas(mapa)
            resultado.linhas.append(nova)
        return resultado

    def __add__(self, outro: ICSV) -> ICSV:
        """Atalho para `concatenar`. Permite usar o operador `+` entre dois ICSV."""
        return self.concatenar(outro)

    def join(
        self,
        outro_csv: ICSV,
        chave_esq: str,
        chave_dir: str,
        tipo: str = 'inner',
        sufixos: tuple[str, str] = ("_esq", "_dir"),
    ) -> ICSV:
        """Realiza um JOIN entre este ICSV e outro, combinando colunas de ambos.

        Usa hash join internamente — o arquivo da direita é indexado em memória,
        tornando a operação eficiente mesmo para arquivos de tamanho médio.
        Colunas com nomes conflitantes recebem sufixos para evitar ambiguidade.

        Args:
            outro_csv: Objeto `ICSV` com o qual realizar o join (lado direito).
            chave_esq: Nome da coluna de junção neste ICSV (lado esquerdo).
            chave_dir: Nome da coluna de junção em `outro_csv` (lado direito).
            tipo: Tipo do join — `'inner'` retorna apenas correspondências;
                  `'left'` mantém todas as linhas da esquerda, preenchendo
                  com strings vazias quando não há correspondência (padrão: 'inner').
            sufixos: Tupla `(sufixo_esq, sufixo_dir)` aplicada a colunas com
                     nomes iguais nos dois arquivos (padrão: `('_esq', '_dir')`).

        Returns:
            Novo `ICSV` com as colunas dos dois arquivos combinadas.

        Raises:
            ValueError: Se algum dos arquivos não tiver cabeçalho, ou se
                        `tipo` não for `'inner'` ou `'left'`.

        ⚠️ Limitação de Memória:
            O arquivo da direita (`outro_csv`) é inteiramente carregado em memória.
            Para arquivos grandes no lado direito, pode ocorrer MemoryError.
            **Boas práticas:** Coloque sempre o arquivo menor no lado direito.
            Se ambos forem grandes, considere filtrar colunas desnecessárias
            ou particionar os dados antes do join.

        Exemplo:
            resultado = pedidos.join(clientes, chave_esq='id_cliente', chave_dir='id', tipo='left')
        """
        if not self.cabecalho or not outro_csv.cabecalho:
            raise ValueError("Ambos os arquivos precisam de cabeçalho para o join.")
        tipo = tipo.strip().lower()
        if tipo not in ('inner', 'left'):
            raise ValueError("O tipo de join deve ser 'inner' ou 'left'.")

        idx_esq = self.cabecalho.obter_indice_da_coluna(chave_esq)
        idx_dir = outro_csv.cabecalho.obter_indice_da_coluna(chave_dir)

        novos_campos: list[str] = []
        indices_dir_importar: list[int] = []
        set_campos_esq = set(self.cabecalho.campos)

        for campo in self.cabecalho.campos:
            if campo in outro_csv.cabecalho.campos and campo != chave_esq:
                novos_campos.append(f"{campo}{sufixos[0]}")
            else:
                novos_campos.append(campo)

        for i, campo in enumerate(outro_csv.cabecalho.campos):
            if campo == chave_dir and chave_dir == chave_esq:
                continue  # evita duplicar a coluna da chave quando os nomes são iguais
            indices_dir_importar.append(i)
            if campo in set_campos_esq:
                novos_campos.append(f"{campo}{sufixos[1]}")
            else:
                novos_campos.append(campo)

        valores_vazios_dir: list[str] = [""] * len(indices_dir_importar)

        resultado = ICSV(delimitador=self.delimitador, quebra_linha=self.quebra_linha)
        resultado.cabecalho = Cabecalho(campos=novos_campos, delimitador=self.delimitador)
        resultado.possui_cabecalho = True
        mapa_colunas = {nome: i for i, nome in enumerate(resultado.cabecalho.campos)}

        # Indexa o lado direito por valor de chave para O(1) por busca
        mapa_hash: dict[str, list[Linha]] = {}
        for linha_dir in outro_csv:
            valor_chave = linha_dir.obter_campo(idx_dir)
            if valor_chave not in mapa_hash:
                mapa_hash[valor_chave] = []
            mapa_hash[valor_chave].append(linha_dir)

        for linha_esq in self:
            valor_busca = linha_esq.obter_campo(idx_esq)
            if valor_busca in mapa_hash:
                for linha_dir in mapa_hash[valor_busca]:
                    valores_dir = [linha_dir.obter_campo(i) for i in indices_dir_importar]
                    nova_linha = Linha(campos=linha_esq.campos + valores_dir, delimitador=self.delimitador)
                    nova_linha.definir_mapa_colunas(mapa_colunas)
                    resultado.linhas.append(nova_linha)
            elif tipo == 'left':
                nova_linha = Linha(campos=linha_esq.campos + valores_vazios_dir, delimitador=self.delimitador)
                nova_linha.definir_mapa_colunas(mapa_colunas)
                resultado.linhas.append(nova_linha)

        return resultado

    # --- Filtragem ---

    def iter_filtrar_por_coluna(
        self,
        nome_coluna: str,
        padrao: str,
        ignorar_maiusculas: bool = True,
    ) -> Iterator[Linha]:
        """Itera linhas filtradas por coluna sem materializar o resultado completo.

        Ideal para arquivos grandes quando usado com `modo_leitura='stream'`.
        """
        if not self.cabecalho:
            raise ValueError("Não é possível filtrar sem um cabeçalho definido.")
        idx = self.cabecalho.obter_indice_da_coluna(nome_coluna)
        comeca_com_pct = padrao.startswith('%')
        termina_com_pct = padrao.endswith('%')
        termo_busca = padrao.strip('%')
        if ignorar_maiusculas:
            termo_busca = termo_busca.lower()

        def bate(linha: Linha) -> bool:
            valor = str(linha.obter_campo(idx))
            valor_normalizado = valor.lower() if ignorar_maiusculas else valor
            if comeca_com_pct and termina_com_pct:
                return termo_busca in valor_normalizado
            if comeca_com_pct:
                return valor_normalizado.endswith(termo_busca)
            if termina_com_pct:
                return valor_normalizado.startswith(termo_busca)
            return valor_normalizado == termo_busca

        for linha in self:
            if bate(linha):
                yield linha

    def iter_filtrar_por_regex(self, nome_coluna: str, padrao: str) -> Iterator[Linha]:
        """Itera linhas filtradas por regex sem materializar o resultado completo."""
        if not self.cabecalho:
            raise ValueError("Não é possível filtrar sem um cabeçalho definido.")
        idx = self.cabecalho.obter_indice_da_coluna(nome_coluna)
        regex = re.compile(padrao)

        for linha in self:
            if regex.search(linha.obter_campo(idx)):
                yield linha

    def salvar_filtrado_por_coluna(
        self,
        novo_caminho: str,
        nome_coluna: str,
        padrao: str,
        ignorar_maiusculas: bool = True,
    ) -> int:
        """Salva em arquivo o filtro por coluna de forma incremental.

        Returns:
            Quantidade de linhas gravadas (sem contar cabeçalho).
        """
        total_gravadas = 0
        with open(novo_caminho, 'w', encoding=self.encoding, newline='') as file:
            escritor = csv.writer(file, delimiter=self.delimitador, lineterminator=self.quebra_linha)
            if self.possui_cabecalho and self.cabecalho:
                escritor.writerow(self.cabecalho.campos)
            for linha in self.iter_filtrar_por_coluna(nome_coluna, padrao, ignorar_maiusculas):
                escritor.writerow(linha.campos)
                total_gravadas += 1
        return total_gravadas

    def salvar_filtrado_por_regex(self, novo_caminho: str, nome_coluna: str, padrao: str) -> int:
        """Salva em arquivo o filtro por regex de forma incremental.

        Returns:
            Quantidade de linhas gravadas (sem contar cabeçalho).
        """
        total_gravadas = 0
        with open(novo_caminho, 'w', encoding=self.encoding, newline='') as file:
            escritor = csv.writer(file, delimiter=self.delimitador, lineterminator=self.quebra_linha)
            if self.possui_cabecalho and self.cabecalho:
                escritor.writerow(self.cabecalho.campos)
            for linha in self.iter_filtrar_por_regex(nome_coluna, padrao):
                escritor.writerow(linha.campos)
                total_gravadas += 1
        return total_gravadas

    def filtrar_por_coluna(
        self,
        nome_coluna: str,
        padrao: str,
        ignorar_maiusculas: bool = True,
    ) -> ICSV:
        """Filtra linhas pelo valor de uma coluna com suporte a padrões estilo SQL LIKE.

        O caractere `%` funciona como coringa, igual ao operador LIKE do SQL.
        A comparação ignora maiúsculas e minúsculas por padrão.

        Args:
            nome_coluna: Nome da coluna a avaliar.
            padrao: Padrão de busca. Exemplos:
                    - `'Ana'`     → correspondência exata
                    - `'Ana%'`   → começa com 'Ana'
                    - `'%Silva'` → termina com 'Silva'
                    - `'%an%'`   → contém 'an' em qualquer posição
            ignorar_maiusculas: Se True (padrão), a comparação é case-insensitive.

        Returns:
            Novo `ICSV` com as linhas que satisfazem o padrão.

        Raises:
            ValueError: Se não houver cabeçalho ou a coluna não existir.
        """
        return self._novo_com_linhas(
            self.iter_filtrar_por_coluna(nome_coluna, padrao, ignorar_maiusculas)
        )

    def filtrar_por_regex(self, nome_coluna: str, padrao: str) -> ICSV:
        """Filtra linhas cujo valor em uma coluna corresponde a um padrão regex.

        Usa `re.search`, portanto o padrão pode corresponder a qualquer
        parte do valor. Para correspondência total, use `^` e `$`.

        Args:
            nome_coluna: Nome da coluna a avaliar.
            padrao: Expressão regular compatível com o módulo `re`.

        Returns:
            Novo `ICSV` com as linhas cujo campo bate o padrão.

        Raises:
            ValueError: Se não houver cabeçalho ou a coluna não existir.
            re.error: Se o padrão regex for inválido.

        Exemplo:
            cpfs_validos = dados.filtrar_por_regex('cpf', r'^\\d{11}$')
            emails = dados.filtrar_por_regex('email', r'@gmail\\.com$')
        """
        return self._novo_com_linhas(self.iter_filtrar_por_regex(nome_coluna, padrao))

    # --- Análise ---

    def valores_unicos(self, nome_coluna: str) -> set[str]:
        """Retorna o conjunto de valores distintos presentes em uma coluna.

        Args:
            nome_coluna: Nome da coluna a analisar.

        Returns:
            `set[str]` com todos os valores únicos encontrados.

        Raises:
            ValueError: Se não houver cabeçalho ou a coluna não existir.
        """
        if not self.cabecalho:
            raise ValueError("Não é possível obter valores únicos sem um cabeçalho definido.")
        idx = self.cabecalho.obter_indice_da_coluna(nome_coluna)
        return {linha.obter_campo(idx) for linha in self}

    def contar_por(self, nome_coluna: str) -> dict[str, int]:
        """Conta as ocorrências de cada valor em uma coluna.

        Args:
            nome_coluna: Nome da coluna a analisar.

        Returns:
            Dicionário `{valor: contagem}` ordenado por ordem de aparição.

        Raises:
            ValueError: Se não houver cabeçalho ou a coluna não existir.

        Exemplo:
            dados.contar_por('status')  # → {'ativo': 120, 'inativo': 45}
        """
        if not self.cabecalho:
            raise ValueError("Não é possível contar sem um cabeçalho definido.")
        idx = self.cabecalho.obter_indice_da_coluna(nome_coluna)
        contagem: dict[str, int] = {}
        for linha in self:
            valor = linha.obter_campo(idx)
            contagem[valor] = contagem.get(valor, 0) + 1
        return contagem

    def deduplicar(self, coluna: str | None = None) -> ICSV:
        """Retorna um novo ICSV sem linhas duplicadas.

        Quando `coluna` é informada, mantém apenas a primeira ocorrência
        de cada valor nessa coluna (útil para deduplicar por chave natural,
        como CPF ou e-mail). Sem `coluna`, remove linhas completamente
        idênticas em todos os campos.

        Args:
            coluna: Nome da coluna a usar como critério de unicidade.
                    Se None, compara todos os campos da linha.

        Returns:
            Novo `ICSV` sem as linhas duplicadas.

        Raises:
            ValueError: Se `coluna` for informada mas não houver cabeçalho.
        """
        vistos: set[Any] = set()
        unicas: list[Linha] = []
        if coluna:
            if not self.cabecalho:
                raise ValueError("Não é possível deduplicar por coluna sem um cabeçalho definido.")
            idx = self.cabecalho.obter_indice_da_coluna(coluna)
            for linha in self:
                chave = linha.obter_campo(idx)
                if chave not in vistos:
                    vistos.add(chave)
                    unicas.append(linha)
        else:
            for linha in self:
                chave = tuple(linha.campos)
                if chave not in vistos:
                    vistos.add(chave)
                    unicas.append(linha)
        return self._novo_com_linhas(unicas)

    def validar_coluna(self, nome_coluna: str, padrao: str) -> list[tuple[int, Linha]]:
        """Valida os valores de uma coluna contra um padrão regex.

        Retorna as linhas que **não** satisfazem o padrão, tornando fácil
        identificar dados inconsistentes antes de processar o arquivo.

        Args:
            nome_coluna: Nome da coluna a validar.
            padrao: Expressão regular que define o formato esperado.

        Returns:
            Lista de tuplas `(indice, linha)` para cada linha inválida.
            Retorna lista vazia se todos os valores forem válidos.

        Raises:
            ValueError: Se não houver cabeçalho ou a coluna não existir.
            re.error: Se o padrão regex for inválido.

        Exemplo:
            erros = dados.validar_coluna('cpf', r'^\\d{11}$')
            for indice, linha in erros:
                print(f"Linha {indice}: CPF inválido → {linha.cpf}")
        """
        if not self.cabecalho:
            raise ValueError("Não é possível validar sem um cabeçalho definido.")
        idx = self.cabecalho.obter_indice_da_coluna(nome_coluna)
        regex = re.compile(padrao)
        if self.modo_leitura == "stream":
            raise RuntimeError(
                "validar_coluna com índice não está disponível em modo_leitura='stream'. "
                "Use iteração e coleta manual para validar em stream."
            )
        return [
            (i, l) for i, l in enumerate(self.linhas)
            if not regex.search(l.obter_campo(idx))
        ]

    # --- Importação ---

    @classmethod
    def from_list_of_dicts(
        cls,
        dados: list[dict[str, Any]],
        delimitador: str = ",",
        quebra_linha: str = "\n",
    ) -> ICSV:
        """Cria um ICSV a partir de uma lista de dicionários.

        As chaves do primeiro dicionário definem os nomes das colunas.
        Chaves ausentes em dicionários subsequentes são preenchidas com
        string vazia.

        Args:
            dados: Lista de dicionários com os dados. Todos devem ter as
                   mesmas chaves (ou ser superconjunto das chaves do primeiro).
            delimitador: Delimitador a usar no objeto resultante (padrão: ',').
            quebra_linha: Quebra de linha a usar no objeto resultante (padrão: '\\n').

        Returns:
            Novo `ICSV` com cabeçalho e linhas populados.

        Exemplo:
            registros = [{"nome": "Ana", "idade": "30"}, {"nome": "Bruno", "idade": "25"}]
            dados = ICSV.from_list_of_dicts(registros)
        """
        if not dados:
            return cls(delimitador=delimitador, quebra_linha=quebra_linha)
        campos_cabecalho = list(dados[0].keys())
        resultado = cls(delimitador=delimitador, quebra_linha=quebra_linha)
        resultado.cabecalho = Cabecalho(campos=campos_cabecalho, delimitador=delimitador)
        resultado.possui_cabecalho = True
        mapa = {nome: i for i, nome in enumerate(campos_cabecalho)}
        for d in dados:
            campos = [str(d.get(col, "")) for col in campos_cabecalho]
            nova = Linha(campos=campos, delimitador=delimitador, quebra_linha=quebra_linha)
            nova.definir_mapa_colunas(mapa)
            resultado.linhas.append(nova)
        return resultado
