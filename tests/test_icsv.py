import pytest
import tempfile
import os
import csv
from icsv import Linha, Cabecalho, BaseICSV, ICSV, Quebra, detectar_quebra_de_linha_texto, detectar_quebra_de_linha_arquivo

# Tests for Linha class
class TestLinha:
    def test_init(self):
        linha = Linha(["a", "b", "c"])
        assert linha.campos == ["a", "b", "c"]
        assert linha.delimitador == ","
        assert linha.quebra_linha == "\n"

    def test_adicionar_campo(self):
        linha = Linha(["a", "b"])
        linha.adicionar_campo("c")
        assert linha.campos == ["a", "b", "c"]

    def test_remover_campo_por_indice(self):
        linha = Linha(["a", "b", "c"])
        linha.remover_campo_por_indice(1)
        assert linha.campos == ["a", "c"]

    def test_remover_campo_por_indice_invalid(self):
        linha = Linha(["a", "b"])
        with pytest.raises(IndexError):
            linha.remover_campo_por_indice(5)

    def test_atualizar_campo(self):
        linha = Linha(["a", "b", "c"])
        linha.atualizar_campo(1, "x")
        assert linha.campos == ["a", "x", "c"]

    def test_atualizar_campo_invalid(self):
        linha = Linha(["a", "b"])
        with pytest.raises(IndexError):
            linha.atualizar_campo(5, "x")

    def test_obter_campo(self):
        linha = Linha(["a", "b", "c"])
        assert linha.obter_campo(1) == "b"

    def test_obter_campo_invalid(self):
        linha = Linha(["a", "b"])
        with pytest.raises(IndexError):
            linha.obter_campo(5)

    def test_numero_de_campos(self):
        linha = Linha(["a", "b", "c"])
        assert linha.numero_de_campos() == 3

    def test_limpar_campos(self):
        linha = Linha(["a", "b", "c"])
        linha.limpar_campos()
        assert linha.campos == []

    def test_existe_campo(self):
        linha = Linha(["a", "b", "c"])
        assert linha.existe_campo("b") == True
        assert linha.existe_campo("d") == False

    def test_to_dict(self):
        linha = Linha(["a", "b"], delimitador=";", quebra_linha="\r\n")
        expected = {"campos": ["a", "b"], "delimitador": ";", "quebra_linha": "\r\n"}
        assert linha.to_dict() == expected

    def test_to_json(self):
        linha = Linha(["a", "b"])
        import json
        assert json.loads(linha.to_json()) == linha.to_dict()

    def test_str(self):
        linha = Linha(["a", "b", "c"])
        assert str(linha) == "a,b,c"

    def test_repr(self):
        linha = Linha(["a", "b"])
        assert repr(linha) == "Linha(campos=['a', 'b'], delimitador=',', quebra_linha='\n')"

    def test_getattr_with_mapa(self):
        mapa = {"nome": 0, "idade": 1}
        linha = Linha(["John", "25"], mapa_colunas=mapa)
        assert linha.nome == "John"
        assert linha.idade == "25"

    def test_getattr_invalid(self):
        linha = Linha(["a", "b"])
        with pytest.raises(AttributeError):
            _ = linha.invalid

    def test_getitem_por_indice(self):
        linha = Linha(["a", "b", "c"])
        assert linha[0] == "a"
        assert linha[2] == "c"

    def test_getitem_por_indice_invalido(self):
        linha = Linha(["a", "b"])
        with pytest.raises(IndexError):
            _ = linha[5]

    def test_getitem_por_nome(self):
        mapa = {"nome": 0, "idade": 1}
        linha = Linha(["Ana", "30"], mapa_colunas=mapa)
        assert linha["nome"] == "Ana"
        assert linha["idade"] == "30"

    def test_getitem_por_nome_case_insensitive(self):
        mapa = {"nome": 0}
        linha = Linha(["Ana"], mapa_colunas=mapa)
        assert linha["Nome"] == "Ana"
        assert linha["NOME"] == "Ana"

    def test_getitem_por_nome_invalido(self):
        linha = Linha(["a", "b"])
        with pytest.raises(KeyError):
            _ = linha["coluna_inexistente"]

    def test_from_dict(self):
        data = {"campos": ["a", "b"], "delimitador": ";", "quebra_linha": "\r\n"}
        linha = Linha.from_dict(data)
        assert linha.campos == ["a", "b"]
        assert linha.delimitador == ";"
        assert linha.quebra_linha == "\r\n"

    def test_from_json(self):
        import json
        data = {"campos": ["a", "b"], "delimitador": ";", "quebra_linha": "\r\n"}
        json_str = json.dumps(data)
        linha = Linha.from_json(json_str)
        assert linha.campos == ["a", "b"]

    def test_eq(self):
        linha1 = Linha(["a", "b"])
        linha2 = Linha(["a", "b"])
        linha3 = Linha(["a", "c"])
        assert linha1 == linha2
        assert linha1 != linha3
        assert linha1 != "not a linha"

# Tests for Cabecalho class
class TestCabecalho:
    def test_init_valid(self):
        cab = Cabecalho(["name", "age"])
        assert cab.campos == ["name", "age"]

    def test_init_duplicate(self):
        with pytest.raises(ValueError):
            Cabecalho(["name", "age", "name"])

    def test_obter_indice_da_coluna(self):
        cab = Cabecalho(["name", "age", "city"])
        assert cab.obter_indice_da_coluna("age") == 1

    def test_obter_indice_da_coluna_invalid(self):
        cab = Cabecalho(["name", "age"])
        with pytest.raises(ValueError):
            cab.obter_indice_da_coluna("invalid")

    def test_repr(self):
        cab = Cabecalho(["a", "b"])
        assert repr(cab) == f"Cabecalho(campos={cab.campos}, delimitador='{cab.delimitador}', quebra_linha={repr(cab.quebra_linha)})"

# Tests for BaseICSV class
class TestBaseICSV:
    def test_init_with_text(self):
        csv_text = "name,age\nJohn,25\nJane,30"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True)
        assert icsv.delimitador == ","
        assert icsv.possui_cabecalho == True
        assert len(icsv.linhas) == 2
        assert icsv.cabecalho.campos == ["name", "age"]

    def test_init_with_file(self):
        csv_text = "name,age\nJohn,25"
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write(csv_text)
            temp_path = f.name
        try:
            icsv = BaseICSV(caminho=temp_path, possui_cabecalho=True)
            assert icsv.cabecalho.campos == ["name", "age"]
            assert len(icsv.linhas) == 1
        finally:
            os.unlink(temp_path)

    def test_init_with_file_cp1252_fallback(self):
        csv_text = "nome;moeda\nAna;€"
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as f:
            f.write(csv_text.encode('cp1252'))
            temp_path = f.name
        try:
            icsv = BaseICSV(caminho=temp_path, possui_cabecalho=True)
            assert icsv.encoding == "cp1252"
            assert icsv.delimitador == ";"
            assert icsv.linhas[0].nome == "Ana"
            assert icsv.linhas[0].moeda == "€"
        finally:
            os.unlink(temp_path)

    def test_init_with_file_iso_8859_1_fallback(self):
        csv_text = "nome;cidade\nJoão;São Luís"
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as f:
            f.write(csv_text.encode('iso-8859-1'))
            temp_path = f.name
        try:
            icsv = BaseICSV(caminho=temp_path, possui_cabecalho=True)
            assert icsv.delimitador == ";"
            assert icsv.linhas[0].nome == "João"
            assert icsv.linhas[0].cidade == "São Luís"
        finally:
            os.unlink(temp_path)

    def test_to_json(self):
        csv_text = "name,age\nJohn,25"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True)
        json_str = icsv.to_json()
        import json
        data = json.loads(json_str)
        assert "cabecalho" in data
        assert "linhas" in data

    def test_info(self):
        csv_text = "name,age\nJohn,25\nJane,30"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True)
        info = icsv.info()
        assert "Número de linhas: 2" in info
        assert "Número de colunas: 2" in info

    def test_info_json(self):
        csv_text = "name,age\nJohn,25"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True)
        info_json = icsv.info_json()
        import json
        data = json.loads(info_json)
        assert data["numero_linhas"] == 1

    def test_info_stream_nao_conta_linhas(self, monkeypatch):
        csv_text = "name,age\nJohn,25\nJane,30"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True, modo_leitura="stream")

        def iterador_falha(self):
            raise RuntimeError("não deveria iterar no info/info_json em stream")

        monkeypatch.setattr(BaseICSV, "_BaseICSV__iterar_linhas_stream", iterador_falha)

        info = icsv.info()
        assert "Número de linhas: Desconhecido (modo stream)" in info
        assert "Número de colunas: 2" in info

        import json
        data = json.loads(icsv.info_json())
        assert data["numero_linhas"] is None
        assert data["numero_colunas"] == 2

    def test_preview(self):
        csv_text = "name,age\nJohn,25"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True)
        preview = icsv.preview()
        assert "name,age" in preview
        assert "John,25" in preview

    def test_str(self):
        icsv = BaseICSV(texto="a,b\n1,2", possui_cabecalho=True)
        assert "icsv(caminho='', possui_cabecalho=True" in str(icsv)

    def test_fallback_delimitador_quando_sniffer_sniff_falha(self, monkeypatch):
        csv_text = "nome;idade\nAna;30\nBruno;40"

        def sniff_falha(self, sample, delimiters=None):
            raise csv.Error("sniffer falhou")

        monkeypatch.setattr(csv.Sniffer, "sniff", sniff_falha)

        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True)

        assert icsv.delimitador == ";"
        assert len(icsv.linhas) == 2
        assert icsv.linhas[0].obter_campo(0) == "Ana"

    def test_sonda_com_amostra_sem_quebra_final(self):
        base = BaseICSV()
        amostra = "col1,col2\nvalor1,valor2\nparcial,sem_fim"
        normalizada = base._BaseICSV__normalizar_amostra_para_sniffer(amostra)

        assert normalizada.endswith("\n")
        assert "parcial,sem_fim" not in normalizada

    def test_avisa_por_padrao_em_linha_menor_que_cabecalho(self):
        csv_text = "nome,idade,cidade\nAna,30\nBruno,40,RJ"

        with pytest.warns(UserWarning, match=r"Linha 2 possui 2 colunas; esperado: 3"):
            icsv = BaseICSV(texto=csv_text, possui_cabecalho=True)

        assert len(icsv.linhas) == 2
        assert icsv.linhas[0].campos == ["Ana", "30"]

    def test_preenche_linha_menor_com_vazio_no_parse(self):
        csv_text = "nome,idade,cidade\nAna,30\nBruno,40,RJ"

        icsv = BaseICSV(
            texto=csv_text,
            possui_cabecalho=True,
            tratamento_linhas_irregulares="preencher",
        )

        assert icsv.linhas[0].campos == ["Ana", "30", ""]
        assert icsv.linhas[0].cidade == ""

    def test_preenche_normaliza_linha_maior_no_parse(self):
        csv_text = "nome,idade\nAna,30,extra"

        icsv = BaseICSV(
            texto=csv_text,
            possui_cabecalho=True,
            tratamento_linhas_irregulares="preencher",
        )

        assert icsv.linhas[0].campos == ["Ana", "30"]

    def test_tratamento_linhas_irregulares_invalido(self):
        with pytest.raises(ValueError):
            BaseICSV(texto="a,b\n1,2", tratamento_linhas_irregulares="invalido")

    def test_stream_mantem_eager_como_padrao(self):
        icsv = BaseICSV(texto="a,b\n1,2", possui_cabecalho=True)
        assert icsv.modo_leitura == "eager"

    def test_stream_itera_sem_materializar(self):
        csv_text = "nome,idade\nAna,30\nBruno,40"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True, modo_leitura="stream")

        linhas = list(icsv)
        assert len(linhas) == 2
        assert linhas[0].nome == "Ana"
        assert linhas[1].idade == "40"

        with pytest.raises(RuntimeError, match="modo_leitura='stream'"):
            _ = icsv.linhas

    def test_stream_preenche_linha_menor_no_parse(self):
        csv_text = "nome,idade,cidade\nAna,30\nBruno,40,RJ"
        icsv = BaseICSV(
            texto=csv_text,
            possui_cabecalho=True,
            modo_leitura="stream",
            tratamento_linhas_irregulares="preencher",
        )

        linhas = list(icsv)
        assert linhas[0].campos == ["Ana", "30", ""]
        assert linhas[0].cidade == ""

    def test_len_stream_estimate_em_stream_texto(self, monkeypatch):
        csv_text = "nome,idade\nAna,30\nBruno,40\nCarla,35"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True, modo_leitura="stream")

        def iterador_falha(self):
            raise RuntimeError("não deveria iterar tudo para estimar")

        monkeypatch.setattr(BaseICSV, "_BaseICSV__iterar_linhas_stream", iterador_falha)
        assert icsv.len_stream_estimate() == 3

    def test_len_stream_estimate_em_eager_retorna_exato(self):
        csv_text = "nome,idade\nAna,30\nBruno,40\nCarla,35"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True, modo_leitura="eager")
        assert icsv.len_stream_estimate() == 3

    def test_len_stream_estimate_retorna_intervalo_em_eager(self):
        csv_text = "nome,idade\nAna,30\nBruno,40\nCarla,35"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True, modo_leitura="eager")
        estimativa, intervalo = icsv.len_stream_estimate(retornar_intervalo=True)
        assert estimativa == 3
        assert intervalo == (3, 3)

    def test_len_stream_estimate_retorna_intervalo_em_stream_texto(self):
        csv_text = "nome,idade\nAna,30\nBruno,40\nCarla,35"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True, modo_leitura="stream")
        estimativa, intervalo = icsv.len_stream_estimate(retornar_intervalo=True)
        assert estimativa == 3
        assert intervalo == (3, 3)

    def test_len_stream_estimate_parametro_invalido(self):
        icsv = BaseICSV(texto="a,b\n1,2", possui_cabecalho=True, modo_leitura="stream")
        with pytest.raises(ValueError):
            icsv.len_stream_estimate(0)

    def test_len_stream_estimate_confianca_invalida_zero(self):
        icsv = BaseICSV(texto="a,b\n1,2\n3,4", possui_cabecalho=True, modo_leitura="stream")
        with pytest.raises(ValueError):
            icsv.len_stream_estimate(confianca=0.0)

    def test_len_stream_estimate_confianca_invalida_um(self):
        icsv = BaseICSV(texto="a,b\n1,2\n3,4", possui_cabecalho=True, modo_leitura="stream")
        with pytest.raises(ValueError):
            icsv.len_stream_estimate(confianca=1.0)

    def test_len_stream_estimate_confianca_aumenta_intervalo(self, tmp_path):
        # Criar arquivo maior com linhas variáveis
        arquivo = tmp_path / "test.csv"
        with open(arquivo, "w", encoding="utf-8", newline="") as f:
            f.write("id,descricao\n")
            for i in range(1, 10001):
                # variar tamanho considerável para gerar variância expressiva
                desc = "x" * ((i * 7) % 200 + 5)
                f.write(f"{i},{desc}\n")

        icsv = BaseICSV(caminho=str(arquivo), possui_cabecalho=True, modo_leitura="stream")
        
        est_80, (inf_80, sup_80) = icsv.len_stream_estimate(
            amostra_linhas=100, retornar_intervalo=True, confianca=0.80
        )
        margem_80 = sup_80 - est_80
        
        est_99, (inf_99, sup_99) = icsv.len_stream_estimate(
            amostra_linhas=100, retornar_intervalo=True, confianca=0.99
        )
        margem_99 = sup_99 - est_99
        
        # Com variância alto no arquivo, margem_99 deve ser maior que margem_80
        assert margem_99 >= margem_80, f"Confiança 99% ({margem_99}) deveria ser >= 80% ({margem_80})"

    def test_modo_leitura_invalido(self):
        with pytest.raises(ValueError):
            BaseICSV(texto="a,b\n1,2", modo_leitura="invalido")

# Tests for ICSV class
class TestICSV:
    def test_order_by_field_name(self):
        csv_text = "name,age\nJohn,25\nJane,30\nBob,20"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        icsv.order_by_field_name("age", cast_type=int)
        assert icsv.linhas[0].obter_campo(1) == "20"
        assert icsv.linhas[1].obter_campo(1) == "25"

    def test_order_by_field_index(self):
        csv_text = "name,age\nJohn,25\nJane,30"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        icsv.order_by_field_index(1, cast_type=int)
        assert icsv.linhas[0].obter_campo(1) == "25"

    def test_salvar(self):
        csv_text = "name,age\nJohn,25"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            temp_path = f.name
        icsv.caminho = temp_path
        icsv.salvar()
        with open(temp_path, 'r') as f:
            content = f.read()
        assert "name,age" in content
        assert "John,25" in content
        os.unlink(temp_path)

    def test_salvar_como(self):
        csv_text = "name,age\nJohn,25"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            temp_path = f.name
        icsv.salvar_como(temp_path)
        with open(temp_path, 'r') as f:
            content = f.read()
        assert "name,age" in content
        os.unlink(temp_path)

    def test_adicionar_linha(self):
        csv_text = "name,age\nJohn,25"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        nova_linha = Linha(["Jane", "30"])
        icsv.adicionar_linha(nova_linha)
        assert len(icsv.linhas) == 2
        assert icsv.linhas[1].campos == ["Jane", "30"]

    def test_adicionar_linha_wrong_columns(self):
        csv_text = "name,age\nJohn,25"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        nova_linha = Linha(["Jane"])  # wrong number
        with pytest.raises(ValueError):
            icsv.adicionar_linha(nova_linha)

    def test_remover_linha(self):
        csv_text = "name,age\nJohn,25\nJane,30"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        icsv.remover_linha(0)
        assert len(icsv.linhas) == 1
        assert icsv.linhas[0].campos == ["Jane", "30"]

    def test_adicionar_coluna(self):
        csv_text = "name,age\nJohn,25"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        icsv.adicionar_coluna("city", "NY")
        assert icsv.cabecalho.campos == ["name", "age", "city"]
        assert icsv.linhas[0].campos == ["John", "25", "NY"]

    def test_remover_coluna(self):
        csv_text = "name,age,city\nJohn,25,NY"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        icsv.remover_coluna("age")
        assert icsv.cabecalho.campos == ["name", "city"]
        assert icsv.linhas[0].campos == ["John", "NY"]

    def test_atualizar_nome_coluna(self):
        csv_text = "name,age\nJohn,25"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        icsv.atualizar_nome_coluna("age", "idade")
        assert icsv.cabecalho.campos == ["name", "idade"]

    def test_join_inner(self):
        csv1 = "id,name\n1,John\n2,Jane"
        csv2 = "id,age\n1,25\n3,30"
        icsv1 = ICSV(texto=csv1, possui_cabecalho=True)
        icsv2 = ICSV(texto=csv2, possui_cabecalho=True)
        result = icsv1.join(icsv2, "id", "id", "inner")
        assert len(result.linhas) == 1
        assert result.linhas[0].campos == ["1", "John", "25"]

    def test_filtrar_por_coluna(self):
        csv_text = "name,age\nJohn,25\nJane,30\nBob,25"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        filtered = icsv.filtrar_por_coluna("age", "25")
        assert len(filtered.linhas) == 2

    def test_iter_filtrar_por_coluna_stream(self):
        csv_text = "name,age\nJohn,25\nJane,30\nBob,25"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True, modo_leitura="stream")
        nomes = [linha.name for linha in icsv.iter_filtrar_por_coluna("age", "25")]
        assert nomes == ["John", "Bob"]

    def test_salvar_filtrado_por_coluna_stream(self):
        csv_text = "name,age\nJohn,25\nJane,30\nBob,25"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True, modo_leitura="stream")

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            temp_path = f.name

        try:
            total = icsv.salvar_filtrado_por_coluna(temp_path, "age", "25")
            assert total == 2

            with open(temp_path, 'r') as f:
                content = f.read()

            assert "name,age" in content
            assert "John,25" in content
            assert "Bob,25" in content
            assert "Jane,30" not in content
        finally:
            os.unlink(temp_path)

    def test_modificar_valores(self):
        csv_text = "name,age\nJohn,25\nJane,30"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        icsv.modificar_valores("age", lambda x: int(x) + 1)
        assert icsv.linhas[0].obter_campo(1) == "26"
        assert icsv.linhas[1].obter_campo(1) == "31"

class TestNovasFuncionalidades:
    def test_len(self):
        icsv = ICSV(texto="name,age\nJohn,25\nJane,30", possui_cabecalho=True)
        assert len(icsv) == 2

    def test_getitem_int(self):
        icsv = ICSV(texto="name,age\nJohn,25\nJane,30", possui_cabecalho=True)
        assert icsv[0].campos == ["John", "25"]

    def test_getitem_slice(self):
        icsv = ICSV(texto="name,age\nA,1\nB,2\nC,3", possui_cabecalho=True)
        result = icsv[0:2]
        assert len(result) == 2
        assert result.cabecalho.campos == ["name", "age"]

    def test_getitem_invalid_type(self):
        icsv = ICSV(texto="name,age\nJohn,25", possui_cabecalho=True)
        import pytest
        with pytest.raises(TypeError):
            _ = icsv["invalid"]

    def test_to_list_of_dicts(self):
        icsv = ICSV(texto="name,age\nJohn,25\nJane,30", possui_cabecalho=True)
        result = icsv.to_list_of_dicts()
        assert result == [{"name": "John", "age": "25"}, {"name": "Jane", "age": "30"}]

    def test_to_list_of_dicts_sem_cabecalho(self):
        icsv = BaseICSV(texto="John,25\nJane,30", possui_cabecalho=False)
        result = icsv.to_list_of_dicts()
        assert result[0] == {"col_0": "John", "col_1": "25"}

    def test_from_list_of_dicts(self):
        dados = [{"nome": "João", "idade": "25"}, {"nome": "Ana", "idade": "30"}]
        icsv = ICSV.from_list_of_dicts(dados)
        assert icsv.cabecalho.campos == ["nome", "idade"]
        assert len(icsv) == 2
        assert icsv[0].nome == "João"

    def test_from_list_of_dicts_vazio(self):
        icsv = ICSV.from_list_of_dicts([])
        assert len(icsv) == 0

    def test_head(self):
        icsv = ICSV(texto="name,age\nA,1\nB,2\nC,3\nD,4\nE,5\nF,6", possui_cabecalho=True)
        result = icsv.head(3)
        assert len(result) == 3
        assert result[0].campos == ["A", "1"]
        assert result.cabecalho.campos == ["name", "age"]

    def test_tail(self):
        icsv = ICSV(texto="name,age\nA,1\nB,2\nC,3\nD,4", possui_cabecalho=True)
        result = icsv.tail(2)
        assert len(result) == 2
        assert result[0].campos == ["C", "3"]

    def test_tail_stream(self):
        icsv = ICSV(texto="name,age\nA,1\nB,2\nC,3\nD,4", possui_cabecalho=True, modo_leitura="stream")
        result = icsv.tail(2)
        assert len(result) == 2
        assert result[0].campos == ["C", "3"]
        assert result[1].campos == ["D", "4"]

    def test_selecionar_colunas(self):
        icsv = ICSV(texto="nome,idade,cidade\nJoão,25,SP\nAna,30,RJ", possui_cabecalho=True)
        result = icsv.selecionar_colunas(["nome", "cidade"])
        assert result.cabecalho.campos == ["nome", "cidade"]
        assert result[0].campos == ["João", "SP"]
        assert len(result[0].campos) == 2

    def test_selecionar_colunas_sem_cabecalho(self):
        icsv = ICSV(texto="João,25", possui_cabecalho=False)
        import pytest
        with pytest.raises(ValueError):
            icsv.selecionar_colunas(["nome"])

    def test_concatenar(self):
        csv1 = ICSV(texto="name,age\nJohn,25", possui_cabecalho=True)
        csv2 = ICSV(texto="name,age\nJane,30", possui_cabecalho=True)
        result = csv1.concatenar(csv2)
        assert len(result) == 2
        assert result[0].campos == ["John", "25"]
        assert result[1].campos == ["Jane", "30"]

    def test_concatenar_cabecalhos_incompativeis(self):
        csv1 = ICSV(texto="name,age\nJohn,25", possui_cabecalho=True)
        csv2 = ICSV(texto="nome,idade\nJane,30", possui_cabecalho=True)
        import pytest
        with pytest.raises(ValueError):
            csv1.concatenar(csv2)

    def test_add_operator(self):
        csv1 = ICSV(texto="name,age\nJohn,25", possui_cabecalho=True)
        csv2 = ICSV(texto="name,age\nJane,30", possui_cabecalho=True)
        result = csv1 + csv2
        assert len(result) == 2

    def test_valores_unicos(self):
        icsv = ICSV(texto="name,status\nA,ativo\nB,inativo\nC,ativo", possui_cabecalho=True)
        assert icsv.valores_unicos("status") == {"ativo", "inativo"}

    def test_contar_por(self):
        icsv = ICSV(texto="name,status\nA,ativo\nB,inativo\nC,ativo", possui_cabecalho=True)
        assert icsv.contar_por("status") == {"ativo": 2, "inativo": 1}

    def test_deduplicar_linhas_identicas(self):
        icsv = ICSV(texto="name,age\nJohn,25\nJohn,25\nJane,30", possui_cabecalho=True)
        result = icsv.deduplicar()
        assert len(result) == 2

    def test_deduplicar_por_coluna(self):
        icsv = ICSV(texto="name,age\nJohn,25\nJohn,26\nJane,30", possui_cabecalho=True)
        result = icsv.deduplicar("name")
        assert len(result) == 2
        assert result[0].campos == ["John", "25"]

    def test_getattr_apos_head(self):
        icsv = ICSV(texto="nome,idade\nJoão,25\nAna,30\nPedro,40", possui_cabecalho=True)
        result = icsv.head(2)
        assert result[0].nome == "João"

    def test_concatenar_preserva_getattr(self):
        csv1 = ICSV(texto="nome,idade\nJoão,25", possui_cabecalho=True)
        csv2 = ICSV(texto="nome,idade\nAna,30", possui_cabecalho=True)
        result = csv1 + csv2
        assert result[1].nome == "Ana"

    def test_filtrar_por_regex_exato(self):
        icsv = ICSV(texto="cpf,nome\n12345678901,Ana\n1234567890X,Bruno\n98765432100,Carlos", possui_cabecalho=True)
        result = icsv.filtrar_por_regex('cpf', r'^\d{11}$')
        assert len(result) == 2
        assert result[0].nome == "Ana"
        assert result[1].nome == "Carlos"

    def test_filtrar_por_regex_contem(self):
        icsv = ICSV(texto="email,nome\nana@gmail.com,Ana\nbruno@empresa.com,Bruno\ncarlos@gmail.com,Carlos", possui_cabecalho=True)
        result = icsv.filtrar_por_regex('email', r'@gmail\.com$')
        assert len(result) == 2

    def test_filtrar_por_regex_sem_match(self):
        icsv = ICSV(texto="codigo,nome\nabc,Ana\ndef,Bruno", possui_cabecalho=True)
        result = icsv.filtrar_por_regex('codigo', r'^\d+$')
        assert len(result) == 0

    def test_filtrar_por_regex_sem_cabecalho(self):
        icsv = ICSV(texto="Ana,25", possui_cabecalho=False)
        import pytest
        with pytest.raises(ValueError):
            icsv.filtrar_por_regex('nome', r'\d+')

    def test_filtrar_por_regex_preserva_getattr(self):
        icsv = ICSV(texto="cpf,nome\n12345678901,Ana\n1234X,Bruno", possui_cabecalho=True)
        result = icsv.filtrar_por_regex('cpf', r'^\d{11}$')
        assert result[0].nome == "Ana"

    def test_iter_filtrar_por_regex_stream(self):
        icsv = ICSV(
            texto="email,nome\nana@gmail.com,Ana\nbruno@empresa.com,Bruno\ncarlos@gmail.com,Carlos",
            possui_cabecalho=True,
            modo_leitura="stream",
        )
        nomes = [linha.nome for linha in icsv.iter_filtrar_por_regex('email', r'@gmail\.com$')]
        assert nomes == ["Ana", "Carlos"]

    def test_validar_coluna_sem_erros(self):
        icsv = ICSV(texto="cpf,nome\n12345678901,Ana\n98765432100,Bruno", possui_cabecalho=True)
        erros = icsv.validar_coluna('cpf', r'^\d{11}$')
        assert erros == []

    def test_validar_coluna_com_erros(self):
        icsv = ICSV(texto="cpf,nome\n12345678901,Ana\n1234X,Bruno\n000,Carlos", possui_cabecalho=True)
        erros = icsv.validar_coluna('cpf', r'^\d{11}$')
        assert len(erros) == 2
        assert erros[0][0] == 1
        assert erros[0][1].nome == "Bruno"
        assert erros[1][0] == 2
        assert erros[1][1].nome == "Carlos"

    def test_validar_coluna_retorna_indice_correto(self):
        icsv = ICSV(texto="email,nome\nok@a.com,A\nruim,B\nok2@a.com,C\ntambemruim,D", possui_cabecalho=True)
        erros = icsv.validar_coluna('email', r'^[\w.+-]+@[\w-]+\.\w+$')
        indices = [i for i, _ in erros]
        assert indices == [1, 3]

    def test_validar_coluna_sem_cabecalho(self):
        icsv = ICSV(texto="Ana,25", possui_cabecalho=False)
        import pytest
        with pytest.raises(ValueError):
            icsv.validar_coluna('nome', r'\d+')


# ---------------------------------------------------------------------------
# Bloco 1 — Features sem nenhum teste
# ---------------------------------------------------------------------------

class TestDetectarQuebraDeLinha:
    def test_detectar_crlf_em_arquivo(self, tmp_path):
        arquivo = tmp_path / "crlf.csv"
        arquivo.write_bytes(b"nome,idade\r\nAna,30\r\n")
        assert detectar_quebra_de_linha_arquivo(str(arquivo)) == Quebra.CRLF

    def test_detectar_lf_em_arquivo(self, tmp_path):
        arquivo = tmp_path / "lf.csv"
        arquivo.write_bytes(b"nome,idade\nAna,30\n")
        assert detectar_quebra_de_linha_arquivo(str(arquivo)) == Quebra.LF

    def test_detectar_cr_em_arquivo(self, tmp_path):
        arquivo = tmp_path / "cr.csv"
        arquivo.write_bytes(b"nome,idade\rAna,30\r")
        assert detectar_quebra_de_linha_arquivo(str(arquivo)) == Quebra.CR

    def test_detectar_indeterminado_em_arquivo(self, tmp_path):
        arquivo = tmp_path / "sem_quebra.csv"
        arquivo.write_bytes(b"semquebra")
        assert detectar_quebra_de_linha_arquivo(str(arquivo)) == Quebra.INDETERMINADO


class TestSalvarFiltradoPorRegex:
    def test_salvar_filtrado_por_regex_basico(self, tmp_path):
        csv_text = "email,nome\nana@gmail.com,Ana\nbruno@empresa.com,Bruno\ncarlos@gmail.com,Carlos"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True, modo_leitura="stream")
        arquivo = str(tmp_path / "saida.csv")
        total = icsv.salvar_filtrado_por_regex(arquivo, "email", r"@gmail\.com$")
        assert total == 2
        with open(arquivo) as f:
            conteudo = f.read()
        assert "email,nome" in conteudo
        assert "ana@gmail.com" in conteudo
        assert "carlos@gmail.com" in conteudo
        assert "bruno@empresa.com" not in conteudo

    def test_salvar_filtrado_por_regex_sem_match(self, tmp_path):
        csv_text = "email,nome\nana@empresa.com,Ana"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        arquivo = str(tmp_path / "vazio.csv")
        total = icsv.salvar_filtrado_por_regex(arquivo, "email", r"@gmail\.com$")
        assert total == 0
        with open(arquivo) as f:
            conteudo = f.read()
        assert "email,nome" in conteudo

    def test_salvar_filtrado_por_regex_retorna_contagem_correta(self, tmp_path):
        linhas = "\n".join(f"user{i}@{'gmail' if i % 2 == 0 else 'empresa'}.com,User{i}"
                           for i in range(1, 11))
        csv_text = f"email,nome\n{linhas}"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        arquivo = str(tmp_path / "filtrado.csv")
        total = icsv.salvar_filtrado_por_regex(arquivo, "email", r"@gmail\.com$")
        assert total == 5


class TestJoinLeft:
    def test_join_left_preserva_linha_sem_match(self):
        csv1 = "id,nome\n1,João\n2,Ana\n3,Carlos"
        csv2 = "id,idade\n1,25\n2,30"
        icsv1 = ICSV(texto=csv1, possui_cabecalho=True)
        icsv2 = ICSV(texto=csv2, possui_cabecalho=True)
        result = icsv1.join(icsv2, "id", "id", "left")
        assert len(result.linhas) == 3
        assert result.linhas[2].campos == ["3", "Carlos", ""]

    def test_join_left_preenche_com_vazio(self):
        csv1 = "id,nome\n1,João\n99,Sem Par"
        csv2 = "id,cidade\n1,SP"
        icsv1 = ICSV(texto=csv1, possui_cabecalho=True)
        icsv2 = ICSV(texto=csv2, possui_cabecalho=True)
        result = icsv1.join(icsv2, "id", "id", "left")
        assert len(result.linhas) == 2
        assert result.linhas[1].cidade == ""

    def test_join_left_com_todos_os_matches(self):
        csv1 = "id,nome\n1,João\n2,Ana"
        csv2 = "id,idade\n1,25\n2,30"
        icsv1 = ICSV(texto=csv1, possui_cabecalho=True)
        icsv2 = ICSV(texto=csv2, possui_cabecalho=True)
        result = icsv1.join(icsv2, "id", "id", "left")
        assert len(result.linhas) == 2
        assert result.linhas[0].campos == ["1", "João", "25"]
        assert result.linhas[1].campos == ["2", "Ana", "30"]

    def test_join_inner_sem_match_retorna_vazio(self):
        csv1 = "id,nome\n1,João"
        csv2 = "id,idade\n99,30"
        icsv1 = ICSV(texto=csv1, possui_cabecalho=True)
        icsv2 = ICSV(texto=csv2, possui_cabecalho=True)
        result = icsv1.join(icsv2, "id", "id", "inner")
        assert len(result.linhas) == 0

    def test_join_tipo_invalido(self):
        csv1 = "id,nome\n1,João"
        csv2 = "id,idade\n1,30"
        icsv1 = ICSV(texto=csv1, possui_cabecalho=True)
        icsv2 = ICSV(texto=csv2, possui_cabecalho=True)
        with pytest.raises(ValueError):
            icsv1.join(icsv2, "id", "id", "outer")

    def test_join_sem_cabecalho_lanca_erro(self):
        csv1 = "1,João"
        csv2 = "id,idade\n1,30"
        icsv1 = ICSV(texto=csv1, possui_cabecalho=False)
        icsv2 = ICSV(texto=csv2, possui_cabecalho=True)
        with pytest.raises(ValueError):
            icsv1.join(icsv2, "id", "id")


# ---------------------------------------------------------------------------
# Bloco 2 — Lacunas em features parcialmente testadas
# ---------------------------------------------------------------------------

class TestFiltrarPorColunaWildcards:
    def test_filtrar_comeca_com(self):
        icsv = ICSV(texto="nome,cidade\nAna,São Paulo\nBruno,Santo André\nCarlos,Rio",
                    possui_cabecalho=True)
        result = icsv.filtrar_por_coluna("cidade", "Santo%")
        assert len(result.linhas) == 1
        assert result.linhas[0].nome == "Bruno"

    def test_filtrar_termina_com(self):
        icsv = ICSV(texto="nome,email\nAna,ana@gmail.com\nBruno,bruno@hotmail.com",
                    possui_cabecalho=True)
        result = icsv.filtrar_por_coluna("email", "%@gmail.com")
        assert len(result.linhas) == 1
        assert result.linhas[0].nome == "Ana"

    def test_filtrar_contem(self):
        icsv = ICSV(texto="nome,status\nAna,ativo\nBruno,inativo\nCarlos,reativado",
                    possui_cabecalho=True)
        result = icsv.filtrar_por_coluna("status", "%ativ%")
        assert len(result.linhas) == 3

    def test_filtrar_case_sensitive(self):
        icsv = ICSV(texto="nome,status\nAna,ATIVO\nBruno,ativo", possui_cabecalho=True)
        result = icsv.filtrar_por_coluna("status", "ativo", ignorar_maiusculas=False)
        assert len(result.linhas) == 1
        assert result.linhas[0].nome == "Bruno"

    def test_filtrar_sem_cabecalho_lanca_erro(self):
        icsv = ICSV(texto="Ana,ativo", possui_cabecalho=False)
        with pytest.raises(ValueError):
            icsv.filtrar_por_coluna("status", "ativo")

    def test_filtrar_comeca_com_case_insensitive(self):
        icsv = ICSV(texto="nome,status\nAna,Ativo\nBruno,inativo", possui_cabecalho=True)
        result = icsv.filtrar_por_coluna("status", "ativ%")
        assert len(result.linhas) == 1
        assert result.linhas[0].nome == "Ana"


class TestEncodings:
    def test_encoding_utf8_sig(self):
        csv_text = "nome,valor\nAna,100"
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as f:
            f.write(csv_text.encode('utf-8-sig'))
            temp_path = f.name
        try:
            icsv = BaseICSV(caminho=temp_path, possui_cabecalho=True)
            assert icsv.cabecalho.campos == ["nome", "valor"]
            assert icsv.linhas[0].nome == "Ana"
            assert icsv.linhas[0].valor == "100"
        finally:
            os.unlink(temp_path)


class TestDelimitadores:
    def test_delimitador_tab(self):
        csv_text = "nome\tidade\nAna\t30\nBruno\t25"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True)
        assert icsv.delimitador == "\t"
        assert icsv.linhas[0].nome == "Ana"
        assert icsv.linhas[1].idade == "25"

    def test_delimitador_pipe(self):
        csv_text = "nome|cidade\nAna|SP\nBruno|RJ"
        icsv = BaseICSV(texto=csv_text, possui_cabecalho=True)
        assert icsv.delimitador == "|"
        assert icsv.linhas[0].nome == "Ana"
        assert icsv.linhas[1].cidade == "RJ"

    def test_delimitador_tab_getattr(self):
        csv_text = "cpf\tnome\n12345678901\tJoão"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        assert icsv.linhas[0].cpf == "12345678901"
        assert icsv.linhas[0].nome == "João"


class TestStreamComArquivo:
    def test_stream_lendo_arquivo_real(self, tmp_path):
        arquivo = tmp_path / "dados.csv"
        arquivo.write_text("nome,idade\nAna,30\nBruno,25", encoding="utf-8")
        icsv = BaseICSV(caminho=str(arquivo), possui_cabecalho=True, modo_leitura="stream")
        linhas = list(icsv)
        assert len(linhas) == 2
        assert linhas[0].nome == "Ana"
        assert linhas[1].idade == "25"

    def test_stream_arquivo_preserva_getattr(self, tmp_path):
        arquivo = tmp_path / "dados.csv"
        arquivo.write_text("cpf,nome\n12345678901,João", encoding="utf-8")
        icsv = ICSV(caminho=str(arquivo), possui_cabecalho=True, modo_leitura="stream")
        linhas = list(icsv)
        assert linhas[0].cpf == "12345678901"
        assert linhas[0].nome == "João"

    def test_stream_arquivo_filtragem_lazy(self, tmp_path):
        arquivo = tmp_path / "dados.csv"
        arquivo.write_text("status,nome\nativo,Ana\ninativo,Bruno\nativo,Carlos",
                           encoding="utf-8")
        icsv = ICSV(caminho=str(arquivo), possui_cabecalho=True, modo_leitura="stream")
        nomes = [l.nome for l in icsv.iter_filtrar_por_coluna("status", "ativo")]
        assert nomes == ["Ana", "Carlos"]


class TestOrderByReverse:
    def test_order_by_field_name_reverse(self):
        csv_text = "name,age\nJohn,25\nJane,30\nBob,20"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        icsv.order_by_field_name("age", reverse=True, cast_type=int)
        assert icsv.linhas[0].obter_campo(1) == "30"
        assert icsv.linhas[2].obter_campo(1) == "20"

    def test_order_by_field_index_reverse(self):
        csv_text = "name,age\nJohn,25\nJane,30\nBob,20"
        icsv = ICSV(texto=csv_text, possui_cabecalho=True)
        icsv.order_by_field_index(1, reverse=True, cast_type=int)
        assert icsv.linhas[0].obter_campo(1) == "30"
        assert icsv.linhas[2].obter_campo(1) == "20"


class TestJoinSufixos:
    def test_join_colunas_conflitantes_usa_sufixos_padrao(self):
        csv1 = "id,nome,cidade\n1,Ana,SP"
        csv2 = "id,nome,estado\n1,Ana Carolina,São Paulo"
        icsv1 = ICSV(texto=csv1, possui_cabecalho=True)
        icsv2 = ICSV(texto=csv2, possui_cabecalho=True)
        result = icsv1.join(icsv2, "id", "id", "inner")
        assert "nome_esq" in result.cabecalho.campos
        assert "nome_dir" in result.cabecalho.campos
        assert result.linhas[0].nome_esq == "Ana"
        assert result.linhas[0].nome_dir == "Ana Carolina"

    def test_join_sufixos_customizados(self):
        csv1 = "id,nome\n1,Ana"
        csv2 = "id,nome\n1,Ana Carolina"
        icsv1 = ICSV(texto=csv1, possui_cabecalho=True)
        icsv2 = ICSV(texto=csv2, possui_cabecalho=True)
        result = icsv1.join(icsv2, "id", "id", "inner", sufixos=("_a", "_b"))
        assert "nome_a" in result.cabecalho.campos
        assert "nome_b" in result.cabecalho.campos
        assert result.linhas[0].nome_a == "Ana"
        assert result.linhas[0].nome_b == "Ana Carolina"


# ---------------------------------------------------------------------------
# Bloco 3 — Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_csv_so_cabecalho_sem_dados(self):
        icsv = ICSV(texto="nome,idade", possui_cabecalho=True)
        assert len(icsv) == 0
        assert icsv.cabecalho.campos == ["nome", "idade"]

    def test_csv_so_cabecalho_head_retorna_vazio(self):
        icsv = ICSV(texto="nome,idade", possui_cabecalho=True)
        result = icsv.head(5)
        assert len(result) == 0
        assert result.cabecalho.campos == ["nome", "idade"]

    def test_head_n_maior_que_total_retorna_tudo(self):
        icsv = ICSV(texto="nome,idade\nAna,30\nBruno,25", possui_cabecalho=True)
        result = icsv.head(100)
        assert len(result) == 2

    def test_tail_n_maior_que_total_retorna_tudo(self):
        icsv = ICSV(texto="nome,idade\nAna,30\nBruno,25", possui_cabecalho=True)
        result = icsv.tail(100)
        assert len(result) == 2

    def test_linha_str_com_quoting_automatico(self):
        linha = Linha(["João, Jr.", "SP"], delimitador=",")
        assert str(linha) == '"João, Jr.",SP'

    def test_linha_str_com_campo_contendo_aspas(self):
        linha = Linha(['diz "olá"', "SP"], delimitador=",")
        resultado = str(linha)
        assert "SP" in resultado
        assert "olá" in resultado

    def test_valores_unicos_sem_cabecalho_lanca_erro(self):
        icsv = ICSV(texto="Ana,ativo", possui_cabecalho=False)
        with pytest.raises(ValueError):
            icsv.valores_unicos("status")

    def test_contar_por_sem_cabecalho_lanca_erro(self):
        icsv = ICSV(texto="Ana,ativo", possui_cabecalho=False)
        with pytest.raises(ValueError):
            icsv.contar_por("status")

    def test_deduplicar_por_coluna_sem_cabecalho_lanca_erro(self):
        icsv = ICSV(texto="Ana,ativo", possui_cabecalho=False)
        with pytest.raises(ValueError):
            icsv.deduplicar("nome")

    def test_filtrar_por_regex_sem_cabecalho_lanca_erro(self):
        icsv = ICSV(texto="Ana,25", possui_cabecalho=False)
        with pytest.raises(ValueError):
            icsv.filtrar_por_regex("nome", r"\d+")

    def test_csv_uma_unica_linha_de_dados(self):
        icsv = ICSV(texto="nome,idade\nAna,30", possui_cabecalho=True)
        assert len(icsv) == 1
        assert icsv[0].nome == "Ana"

    def test_join_left_sem_nenhum_match_preenche_tudo(self):
        csv1 = "id,nome\n1,Ana\n2,Bruno"
        csv2 = "id,cidade\n99,SP"
        icsv1 = ICSV(texto=csv1, possui_cabecalho=True)
        icsv2 = ICSV(texto=csv2, possui_cabecalho=True)
        result = icsv1.join(icsv2, "id", "id", "left")
        assert len(result.linhas) == 2
        assert result.linhas[0].cidade == ""
        assert result.linhas[1].cidade == ""


if __name__ == "__main__":
    #python -m pytest test_icsv.py -v
    pass