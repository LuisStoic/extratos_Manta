"""
Testes de classificar(): matriz de issues e confiabilidade.

Foco desta iteração: distinção data_ausente vs data_invalida
(introduzida no fix v9 para evitar que '00/00/0000' vá para Grupo A).
"""
import pytest

from app import classificar


def _row(**kw):
    """Lançamento normalizado base: todos os campos válidos e unidade confirmada."""
    base = {
        'Data':      '2026-01-15',
        'Data_Raw':  '15/01/2026',
        'Valor':     100.0,
        'Tipo':      'entrada',
        'Descricao': 'TED RECEBIDO',
    }
    base.update(kw)
    return base


# ============================================================
# Grupo A (caminho feliz)
# ============================================================

class TestGrupoA:

    def test_tudo_ok_vai_para_a(self):
        grupo, conf, issues = classificar(_row(), 100, True)
        assert grupo == 'A'
        assert conf == 'ALTA'
        assert issues == []


# ============================================================
# Distinção data_ausente vs data_invalida
# ============================================================

class TestDataAusenteVsInvalida:

    def test_data_ausente_quando_raw_vazio(self):
        """Fonte sem campo de data → data_ausente."""
        _, _, issues = classificar(_row(Data=None, Data_Raw=''), 100, True)
        assert 'data_ausente' in issues
        assert 'data_invalida' not in issues

    def test_data_invalida_quando_raw_preenchido(self):
        """Fonte tinha dado, mas não parseável → data_invalida."""
        _, _, issues = classificar(_row(Data=None, Data_Raw='00/00/0000'), 100, True)
        assert 'data_invalida' in issues
        assert 'data_ausente' not in issues

    def test_ambas_levam_a_grupo_b_e_baixa(self):
        for raw in ('', '00/00/0000'):
            grupo, conf, _ = classificar(_row(Data=None, Data_Raw=raw), 100, True)
            assert grupo == 'B'
            assert conf == 'BAIXA'

    def test_data_valida_nenhum_issue_de_data(self):
        _, _, issues = classificar(
            _row(Data='2026-01-15', Data_Raw='15/01/2026'), 100, True
        )
        assert 'data_ausente'  not in issues
        assert 'data_invalida' not in issues

    def test_data_raw_so_whitespace_conta_como_ausente(self):
        """Raw com só espaços → tratado como ausente, não inválida."""
        _, _, issues = classificar(_row(Data=None, Data_Raw='   '), 100, True)
        assert 'data_ausente' in issues
        assert 'data_invalida' not in issues


# ============================================================
# Outras issues (regressão)
# ============================================================

class TestOutrasIssues:

    def test_valor_ausente(self):
        _, _, issues = classificar(_row(Valor=None), 100, True)
        assert 'valor_ausente' in issues

    def test_descricao_ausente(self):
        for vazia in ('', '   '):
            _, _, issues = classificar(_row(Descricao=vazia), 100, True)
            assert 'descricao_ausente' in issues

    def test_tipo_indefinido_vira_media(self):
        """tipo_indefinido sozinho → confiabilidade MÉDIA."""
        grupo, conf, issues = classificar(_row(Tipo='indefinido'), 100, True)
        assert grupo == 'B'
        assert conf == 'MÉDIA'
        assert issues == ['tipo_indefinido']

    def test_unidade_incerta_quando_fuzzy_baixo_e_nao_confirmada(self):
        _, _, issues = classificar(_row(), 50, False)
        assert 'unidade_incerta' in issues

    def test_unidade_confirmada_anula_incerteza_fuzzy(self):
        """Mesmo com fuzzy 0, confirmação manual elimina unidade_incerta."""
        _, _, issues = classificar(_row(), 0, True)
        assert 'unidade_incerta' not in issues

    def test_fuzzy_alto_nao_precisa_confirmacao(self):
        _, _, issues = classificar(_row(), 85, False)
        assert 'unidade_incerta' not in issues


# ============================================================
# Composição de issues múltiplas
# ============================================================

class TestMultiplasIssues:

    def test_varias_issues_sobem_para_baixa(self):
        _, conf, issues = classificar(
            _row(Data=None, Data_Raw='', Valor=None, Descricao=''),
            50, False,
        )
        assert conf == 'BAIXA'
        assert {'data_ausente', 'valor_ausente', 'descricao_ausente',
                'unidade_incerta'} <= set(issues)

    def test_data_invalida_com_valor_ausente(self):
        _, _, issues = classificar(
            _row(Data=None, Data_Raw='??/??/????', Valor=None),
            100, True,
        )
        assert 'data_invalida' in issues
        assert 'valor_ausente' in issues
        assert 'data_ausente' not in issues
