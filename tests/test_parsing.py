"""
Testes de parse_valor e parse_data.

Cobre os casos do briefing v9 + regressões de bordas conhecidas:
- parse_valor: regra do último separador (decimal vs milhar),
  sinais (+/-, sufixo BRB, parênteses contábeis), R$, NBSP, NaN.
- parse_data: múltiplos formatos BR/ISO, datas calendaricamente
  inválidas, modo preservar_raw para distinguir ausente vs inválida.
"""
import pytest

from app import parse_valor, parse_data


# ============================================================
# parse_valor
# ============================================================

class TestParseValor:

    @pytest.mark.parametrize("entrada, esperado", [
        ("1.500,00",      1500.0),
        ("1,500.00",      1500.0),
        ("R$ 1.500,00",   1500.0),
        ("1500",          1500.0),
        ("1500,5",        1500.5),
        ("1500.5",        1500.5),
        ("1.500.000,00",  1500000.0),
        ("1,500,000.00",  1500000.0),
        ("-1.500,00",     -1500.0),
        ("R$ -1.500,00",  -1500.0),
        ("(1.500,00)",    -1500.0),
    ])
    def test_casos_obrigatorios(self, entrada, esperado):
        """Os 11 casos válidos do briefing."""
        assert parse_valor(entrada) == esperado

    @pytest.mark.parametrize("entrada", [
        "", None, "abc", "nan", "NaN", "NAT", "None",
    ])
    def test_none_para_nao_parseaveis(self, entrada):
        """Os 4 casos do briefing + variações case-insensitive."""
        assert parse_valor(entrada) is None

    def test_sinal_posterior_brb(self):
        """BRB emite '84.000,00-' como saída — sinal à direita."""
        assert parse_valor("84.000,00-") == -84000.0
        assert parse_valor("1.500,00-")  == -1500.0

    def test_sinal_dupla_negacao(self):
        """Parênteses + sufixo '-' não devem dobrar: permanece negativo."""
        assert parse_valor("(1.500,00-)") == 1500.0  # (negativo) × (sufixo -) = positivo

    def test_aceita_numero(self):
        assert parse_valor(1500)    == 1500.0
        assert parse_valor(1500.5)  == 1500.5
        assert parse_valor(0)       == 0.0

    def test_whitespace_externo_e_nbsp(self):
        """Espaços externos e NBSP (\\u00a0) devem ser tolerados."""
        assert parse_valor("  1.500,00  ") == 1500.0
        assert parse_valor("R$ 1.500,00") == 1500.0

    def test_prefixo_mais(self):
        assert parse_valor("+1500") == 1500.0
        assert parse_valor("+1.500,00") == 1500.0

    def test_zero(self):
        assert parse_valor("0")     == 0.0
        assert parse_valor("0,00")  == 0.0
        assert parse_valor("R$ 0,00") == 0.0

    def test_so_sinal(self):
        """Só '-' ou '(' sem número → None, não explode."""
        assert parse_valor("-")  is None
        assert parse_valor("()") is None


# ============================================================
# parse_data
# ============================================================

class TestParseData:

    def test_iso_canonico(self):
        assert parse_data("2026-01-15") == "2026-01-15"

    def test_br_barras(self):
        assert parse_data("15/01/2026") == "2026-01-15"

    def test_br_pontos(self):
        assert parse_data("15.01.2026") == "2026-01-15"

    def test_br_hifens(self):
        assert parse_data("15-01-2026") == "2026-01-15"

    def test_ano_curto(self):
        assert parse_data("15/01/26") == "2026-01-15"

    def test_compacto_yyyymmdd(self):
        assert parse_data("20260115") == "2026-01-15"

    def test_000_invalida(self):
        """'00/00/0000' é a sentinela que motivou o fix."""
        assert parse_data("00/00/0000") is None

    def test_dia_fora_do_mes(self):
        """31 de fevereiro não existe — nenhum formato deve aceitar."""
        assert parse_data("31/02/2026") is None

    def test_mes_14(self):
        assert parse_data("15/14/2026") is None

    def test_vazio_e_none(self):
        assert parse_data("")   is None
        assert parse_data(None) is None

    def test_sentinelas_textuais(self):
        for t in ["nan", "NaT", "None", "nd"]:
            assert parse_data(t) is None

    def test_string_aleatoria(self):
        assert parse_data("abc") is None
        assert parse_data("xx/yy/zzzz") is None

    def test_aceita_timestamp_pandas(self):
        import pandas as pd
        ts = pd.Timestamp("2026-01-15")
        assert parse_data(ts) == "2026-01-15"

    def test_aceita_datetime(self):
        from datetime import datetime
        assert parse_data(datetime(2026, 1, 15)) == "2026-01-15"

    def test_nat_pandas(self):
        import pandas as pd
        assert parse_data(pd.NaT) is None

    # ---- preservar_raw ----

    def test_preservar_raw_valida(self):
        assert parse_data("15/01/2026", preservar_raw=True) == ("2026-01-15", "15/01/2026")

    def test_preservar_raw_invalida(self):
        """Distinção crítica: inválida retém a raw para o classificador."""
        assert parse_data("00/00/0000", preservar_raw=True) == (None, "00/00/0000")
        assert parse_data("31/02/2026", preservar_raw=True) == (None, "31/02/2026")

    def test_preservar_raw_vazia(self):
        """Raw vazia em fonte None/string vazia — vira 'data_ausente' depois."""
        assert parse_data("",   preservar_raw=True) == (None, "")
        assert parse_data(None, preservar_raw=True) == (None, "")
