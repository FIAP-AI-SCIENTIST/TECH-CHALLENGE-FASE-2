"""Testes do módulo silver.transform — limpeza, dedup, agrupamento e SCD2."""

import pyarrow as pa

from silver.transform import (
    apply_scd2,
    clean,
    dedupe,
    group_by_ano,
    integrate_alfabetizacao_municipio,
    normalize_key,
)


class TestNormalizeKey:
    """Verifica normalização de id_municipio (7 dígitos IBGE)."""

    def test_pads_short_code(self):
        assert normalize_key("550308") == "0550308"

    def test_keeps_already_7_digits(self):
        assert normalize_key("3550308") == "3550308"

    def test_none_returns_none(self):
        assert normalize_key(None) is None

    def test_empty_string_returns_none(self):
        assert normalize_key("") is None
        assert normalize_key("   ") is None

    def test_non_numeric_returns_none(self):
        assert normalize_key("abc1234") is None

    def test_too_many_digits_returns_none(self):
        assert normalize_key("123456789") is None


class TestClean:
    """Verifica tradução de código, normalização de chave e rejeição."""

    def test_normalizes_id_municipio_and_translates_rede(self):
        tabela = pa.table({
            "ano": [2023],
            "id_municipio": ["550308"],
            "serie": ["2"],
            "rede": ["2"],
            "taxa_alfabetizacao": [80.0],
        })
        referencias = {
            "rede": {"2": "Estadual"},
            "serie": {"2": "2° ano do Ensino Fundamental"},
            "diretorio_municipio": {
                "0550308": {"nome": "São Paulo", "sigla_uf": "SP", "nome_regiao": "Sudeste", "capital_uf": 1}
            },
        }

        limpa, rejeitadas = clean("municipio", tabela, referencias)

        assert rejeitadas == 0
        assert limpa.column("id_municipio").to_pylist() == ["0550308"]
        assert limpa.column("rede_desc").to_pylist() == ["Estadual"]
        assert limpa.column("nome").to_pylist() == ["São Paulo"]
        assert limpa.column("sigla_uf").to_pylist() == ["SP"]

    def test_rejects_invalid_id_municipio(self):
        tabela = pa.table({
            "ano": [2023, 2023],
            "id_municipio": ["550308", "abc"],
            "serie": ["2", "2"],
            "rede": ["2", "2"],
        })

        limpa, rejeitadas = clean("municipio", tabela, {})

        assert rejeitadas == 1
        assert limpa.num_rows == 1
        assert limpa.column("id_municipio").to_pylist() == ["0550308"]

    def test_empty_table_returns_empty(self):
        tabela = pa.table({"ano": pa.array([], type=pa.int64())})
        limpa, rejeitadas = clean("uf", tabela, {})
        assert limpa.num_rows == 0
        assert rejeitadas == 0

    def test_alunos_booleans_cast_from_code_strings(self):
        tabela = pa.table({
            "ano": [2023, 2023],
            "id_municipio": ["3550308", "3550308"],
            "id_aluno": ["a1", "a2"],
            "alfabetizado": ["1", "0"],
            "presenca": ["1", "1"],
            "preenchimento_caderno": ["1", "1"],
        })

        limpa, _ = clean("alunos", tabela, {})

        assert limpa.column("alfabetizado").to_pylist() == [True, False]
        assert limpa.column("presenca").to_pylist() == [True, True]


class TestDedupe:
    """Verifica deduplicação por chave natural."""

    def test_removes_duplicate_streaming_reentries(self):
        tabela = pa.table({
            "ano": [2023, 2023, 2023],
            "id_aluno": ["a1", "a1", "a2"],
            "proficiencia": [500.0, 500.0, 600.0],
        })

        result = dedupe("alunos", tabela)

        assert result.num_rows == 2
        assert sorted(result.column("id_aluno").to_pylist()) == ["a1", "a2"]

    def test_keeps_last_occurrence_on_tie(self):
        tabela = pa.table({
            "ano": [2023, 2023],
            "id_aluno": ["a1", "a1"],
            "proficiencia": [500.0, 999.0],
        })

        result = dedupe("alunos", tabela)

        assert result.num_rows == 1
        assert result.column("proficiencia").to_pylist() == [999.0]

    def test_empty_table_returns_empty(self):
        tabela = pa.table({"ano": pa.array([], type=pa.int64()), "id_aluno": pa.array([], type=pa.string())})
        result = dedupe("alunos", tabela)
        assert result.num_rows == 0


class TestGroupByAno:
    """Verifica reagrupamento por ano (batch + streaming misturados)."""

    def test_separates_multiple_years(self):
        tabela = pa.table({
            "ano": [2023, 2024, 2023],
            "sigla_uf": ["SP", "SP", "RJ"],
        })

        grupos = group_by_ano(tabela)

        assert set(grupos.keys()) == {2023, 2024}
        assert grupos[2023].num_rows == 2
        assert grupos[2024].num_rows == 1

    def test_empty_table_returns_empty_dict(self):
        tabela = pa.table({"ano": pa.array([], type=pa.int64())})
        assert group_by_ano(tabela) == {}


class TestApplyScd2:
    """Verifica versionamento SCD Tipo 2."""

    _SCHEMA = pa.schema([
        pa.field("sigla_uf", pa.string()),
        pa.field("rede", pa.string()),
        pa.field("meta_alfabetizacao_2024", pa.float64()),
        pa.field("meta_alfabetizacao_2025", pa.float64()),
        pa.field("meta_alfabetizacao_2026", pa.float64()),
        pa.field("meta_alfabetizacao_2027", pa.float64()),
        pa.field("meta_alfabetizacao_2028", pa.float64()),
        pa.field("meta_alfabetizacao_2029", pa.float64()),
        pa.field("meta_alfabetizacao_2030", pa.float64()),
        pa.field("percentual_participacao", pa.float64()),
        pa.field("taxa_alfabetizacao", pa.float64()),
    ])

    def _make_incoming(self, sigla_uf="SP", rede="0", meta_2024=50.0, taxa=40.0):
        return pa.Table.from_pylist([{
            "sigla_uf": sigla_uf,
            "rede": rede,
            "meta_alfabetizacao_2024": meta_2024,
            "meta_alfabetizacao_2025": 55.0,
            "meta_alfabetizacao_2026": 60.0,
            "meta_alfabetizacao_2027": 65.0,
            "meta_alfabetizacao_2028": 70.0,
            "meta_alfabetizacao_2029": 80.0,
            "meta_alfabetizacao_2030": 100.0,
            "percentual_participacao": 90.0,
            "taxa_alfabetizacao": taxa,
        }], schema=self._SCHEMA)

    def test_new_key_opens_first_version(self):
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        incoming = self._make_incoming()

        result = apply_scd2("meta_alfabetizacao_uf", dimension_vazia, incoming, ano=2023)

        assert result.num_rows == 1
        row = result.to_pylist()[0]
        assert row["valid_from"] == 2023
        assert row["valid_to"] is None
        assert row["is_current"] is True

    def test_reextraction_with_same_values_does_not_version(self):
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        primeira = apply_scd2("meta_alfabetizacao_uf", dimension_vazia, self._make_incoming(), ano=2023)

        segunda = apply_scd2("meta_alfabetizacao_uf", primeira, self._make_incoming(), ano=2024)

        assert segunda.num_rows == 1  # nao duplicou versao
        row = segunda.to_pylist()[0]
        assert row["valid_from"] == 2023  # continua a versao original
        assert row["is_current"] is True

    def test_changed_result_alone_opens_new_version(self):
        """Trajetória de metas e participação idênticas, resultado observado
        diferente: tem que abrir versão nova.

        Regressão: enquanto só a trajetória de metas era comparada, este caso
        caía no ramo "sem mudança" e a linha do ano corrente era descartada
        inteira — a cadeia ficava com a taxa de alfabetização do ano anterior e
        sem nenhuma linha para o ano novo, defasando o fato de meta x resultado
        exatamente na medida que ele existe para comparar.
        """
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        primeira = apply_scd2(
            "meta_alfabetizacao_uf", dimension_vazia, self._make_incoming(taxa=40.0), ano=2023
        )

        segunda = apply_scd2(
            "meta_alfabetizacao_uf", primeira, self._make_incoming(taxa=65.0), ano=2024
        )

        assert segunda.num_rows == 2
        rows = sorted(segunda.to_pylist(), key=lambda r: r["valid_from"])
        assert rows[0]["valid_from"] == 2023
        assert rows[0]["valid_to"] == 2024
        assert rows[0]["is_current"] is False
        assert rows[0]["taxa_alfabetizacao"] == 40.0
        assert rows[1]["valid_from"] == 2024
        assert rows[1]["valid_to"] is None
        assert rows[1]["is_current"] is True
        assert rows[1]["taxa_alfabetizacao"] == 65.0
        # A trajetória de metas continua idêntica nas duas versões — o que
        # mudou foi só o resultado.
        assert rows[0]["meta_alfabetizacao_2030"] == rows[1]["meta_alfabetizacao_2030"]

    def test_changed_value_closes_old_and_opens_new_version(self):
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        primeira = apply_scd2("meta_alfabetizacao_uf", dimension_vazia, self._make_incoming(meta_2024=50.0), ano=2023)

        segunda = apply_scd2(
            "meta_alfabetizacao_uf", primeira, self._make_incoming(meta_2024=99.0), ano=2024
        )

        assert segunda.num_rows == 2
        rows = sorted(segunda.to_pylist(), key=lambda r: r["valid_from"])
        assert rows[0]["valid_from"] == 2023
        assert rows[0]["valid_to"] == 2024
        assert rows[0]["is_current"] is False
        assert rows[1]["valid_from"] == 2024
        assert rows[1]["valid_to"] is None
        assert rows[1]["is_current"] is True
        assert rows[1]["meta_alfabetizacao_2024"] == 99.0

    def test_at_most_one_current_version_per_key(self):
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        primeira = apply_scd2("meta_alfabetizacao_uf", dimension_vazia, self._make_incoming(meta_2024=1.0), ano=2020)
        segunda = apply_scd2("meta_alfabetizacao_uf", primeira, self._make_incoming(meta_2024=2.0), ano=2021)
        terceira = apply_scd2("meta_alfabetizacao_uf", segunda, self._make_incoming(meta_2024=3.0), ano=2022)

        currents = [r for r in terceira.to_pylist() if r["is_current"]]
        assert len(currents) == 1
        assert currents[0]["meta_alfabetizacao_2024"] == 3.0

    def test_key_missing_from_incoming_stays_unchanged(self):
        dimension_vazia = pa.Table.from_pylist([], schema=self._SCHEMA)
        primeira = apply_scd2("meta_alfabetizacao_uf", dimension_vazia, self._make_incoming(sigla_uf="SP"), ano=2023)

        # incoming desta vez só traz RJ — SP não aparece
        outro_incoming = self._make_incoming(sigla_uf="RJ")
        segunda = apply_scd2("meta_alfabetizacao_uf", primeira, outro_incoming, ano=2024)

        siglas = {r["sigla_uf"] for r in segunda.to_pylist()}
        assert siglas == {"SP", "RJ"}
        sp_row = next(r for r in segunda.to_pylist() if r["sigla_uf"] == "SP")
        assert sp_row["is_current"] is True  # nao foi tocada

    def _replay_dos_anos(self):
        """Reproduz o loop de `silver.pipeline.run_silver`: parte de dimensão
        vazia e aplica os anos do Bronze em ordem cronológica.
        """
        dimensao = pa.Table.from_pylist([], schema=self._SCHEMA)
        for ano, meta in ((2023, 50.0), (2024, 99.0)):
            dimensao = apply_scd2(
                "meta_alfabetizacao_uf", dimensao, self._make_incoming(meta_2024=meta), ano=ano
            )
        return dimensao

    def test_replay_from_empty_is_deterministic(self):
        """A tabela SCD2 é função determinística do Bronze. Dois
        replays do mesmo Bronze produzem a mesma tabela — é o que permite
        reconstruir a cadeia a cada execução em vez de acumular versões.
        """
        primeiro = self._replay_dos_anos()
        segundo = self._replay_dos_anos()

        assert segundo.to_pylist() == primeiro.to_pylist()

    def test_no_version_closes_before_it_opens(self):
        """Invariante estrutural do SCD2: `valid_to` nunca pode ser anterior a
        `valid_from` — uma versão fechada antes de abrir não é histórico, é
        corrupção. Era o que a leitura do estado persistido produzia
        (`valid_from=2024, valid_to=2023`) ao comparar o ano mais antigo do
        replay contra a versão vigente deixada pelo run anterior.
        """
        dimensao = self._replay_dos_anos()

        for row in dimensao.to_pylist():
            if row["valid_to"] is not None:
                assert row["valid_to"] >= row["valid_from"], row


class TestIntegrateAlfabetizacaoMunicipio:
    """JOIN real entre indicador observado e trajetória de meta (SCD2) —
    fontes distintas cruzadas na chave territorial com versão vigente por ano."""

    def _indicador(self) -> pa.Table:
        return pa.table({
            "ano": [2023, 2024, 2025],
            "id_municipio": ["3550308", "3550308", "3550308"],
            "rede": ["1", "1", "1"],
            "serie": ["2", "2", "2"],
            "taxa_alfabetizacao": [70.0, 80.0, 85.0],
            "media_portugues": [650.0, 700.0, 710.0],
        })

    def _meta_duas_versoes(self) -> pa.Table:
        """v1 vigente em 2023; v2 abre em 2024 e permanece vigente."""
        return pa.table({
            "ano": [2023, 2024],
            "id_municipio": ["3550308", "3550308"],
            "rede": ["1", "1"],
            "meta_alfabetizacao_2024": [70.0, 75.0],
            "meta_alfabetizacao_2025": [72.0, 78.0],
            "percentual_participacao": [90.0, 95.0],
            "nivel_alfabetizacao": pa.array([1, 2], type=pa.int64()),
            "valid_from": pa.array([2023, 2024], type=pa.int64()),
            "valid_to": pa.array([2024, None], type=pa.int64()),
            "is_current": [False, True],
        })

    def test_join_temporal_escolhe_versao_vigente_do_ano(self):
        integrada = integrate_alfabetizacao_municipio(self._indicador(), self._meta_duas_versoes())
        por_ano = {r["ano"]: r for r in integrada.to_pylist()}
        assert por_ano[2023]["percentual_participacao"] == 90.0  # v1
        assert por_ano[2024]["percentual_participacao"] == 95.0  # v2
        assert por_ano[2024]["meta_indicador"] == 75.0
        assert por_ano[2024]["nivel_alfabetizacao"] == 2

    def test_ano_sem_versao_nova_herda_versao_anterior(self):
        """2025 não tem linha própria na SCD2 — herda v2 (valid_to NULL)."""
        integrada = integrate_alfabetizacao_municipio(self._indicador(), self._meta_duas_versoes())
        por_ano = {r["ano"]: r for r in integrada.to_pylist()}
        assert por_ano[2025]["percentual_participacao"] == 95.0
        assert por_ano[2025]["meta_indicador"] == 78.0

    def test_ano_fora_do_horizonte_da_meta_tem_meta_indicador_null(self):
        """2023 é anterior a 2024 — não existe coluna meta_alfabetizacao_2023,
        então meta_indicador é NULL (não há meta definida para aquele ano)."""
        integrada = integrate_alfabetizacao_municipio(self._indicador(), self._meta_duas_versoes())
        por_ano = {r["ano"]: r for r in integrada.to_pylist()}
        assert por_ano[2023]["meta_indicador"] is None

    def test_municipio_sem_meta_tem_colunas_de_meta_null(self):
        indicador = pa.table({
            "ano": [2024], "id_municipio": ["9999999"], "rede": ["1"], "serie": ["2"],
            "taxa_alfabetizacao": [80.0],
        })
        integrada = integrate_alfabetizacao_municipio(indicador, self._meta_duas_versoes())
        row = integrada.to_pylist()[0]
        assert row["taxa_alfabetizacao"] == 80.0
        assert row["meta_indicador"] is None
        assert row["percentual_participacao"] is None
        assert row["nivel_alfabetizacao"] is None

    def test_meta_ausente_gera_colunas_de_meta_null(self):
        """Sem a tabela de meta processada, o indicador sobrevive íntegro —
        ausência de meta é achado analítico, não motivo para derrubar o pipeline."""
        integrada = integrate_alfabetizacao_municipio(self._indicador(), None)
        assert integrada.num_rows == 3
        for row in integrada.to_pylist():
            assert row["meta_indicador"] is None
            assert row["percentual_participacao"] is None

    def test_meta_eh_broadcast_para_todas_as_series(self):
        """A meta municipal não tem série — o mesmo valor vale para cada série
        do indicador (grão mais fino recebe a meta do grão mais grosso)."""
        indicador = pa.table({
            "ano": [2024, 2024],
            "id_municipio": ["3550308", "3550308"],
            "rede": ["1", "1"],
            "serie": ["1", "2"],
            "taxa_alfabetizacao": [80.0, 85.0],
        })
        integrada = integrate_alfabetizacao_municipio(indicador, self._meta_duas_versoes())
        assert integrada.num_rows == 2
        for row in integrada.to_pylist():
            assert row["meta_indicador"] == 75.0

    def test_grao_preservado_um_a_um(self):
        integrada = integrate_alfabetizacao_municipio(self._indicador(), self._meta_duas_versoes())
        assert integrada.num_rows == 3
        graos = [(r["ano"], r["id_municipio"], r["rede"], r["serie"]) for r in integrada.to_pylist()]
        assert len(graos) == len(set(graos))

    def test_indicador_vazio_retorna_vazio(self):
        vazio = pa.Table.from_pydict({})
        integrada = integrate_alfabetizacao_municipio(vazio, self._meta_duas_versoes())
        assert integrada.num_rows == 0
        assert integrada.column_names == []
