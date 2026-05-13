"""
Testes de resolver_conta e validar_cruzado.

A matriz de 5 casos do briefing v9 está coberta em TestValidarCruzado.
O caso 'modo_legado' é testado separadamente.
"""
import pytest

from resolucao_conta import resolver_conta, validar_cruzado


# ============================================================
# Fixtures de contas cadastradas
# ============================================================

@pytest.fixture
def contas():
    return [
        {
            'id': 'STONE_ASTCU', 'banco_nome': 'Stone', 'banco_codigo': '197',
            'agencia': '0001', 'conta': '1234567', 'cnpj_titular': '00.000.000/0001-00',
            'unit_id': 'RCA', 'ativo_desde': '2025-01-01', 'ativo_ate': None,
        },
        {
            'id': 'BB_MN712', 'banco_nome': 'BB', 'banco_codigo': '001',
            'agencia': '3791-1', 'conta': '40102-5', 'cnpj_titular': '11.111.111/0001-11',
            'unit_id': 'MN712', 'ativo_desde': '2025-01-01', 'ativo_ate': None,
        },
        {
            'id': 'BB_ENCERRADA', 'banco_nome': 'BB', 'banco_codigo': '001',
            'agencia': '9999', 'conta': '99999', 'cnpj_titular': '22.222.222/0001-22',
            'unit_id': 'MN303', 'ativo_desde': '2024-01-01', 'ativo_ate': '2024-12-31',
        },
    ]


# ============================================================
# resolver_conta
# ============================================================

class TestResolverConta:

    def test_match_completo(self, contas):
        det = {'banco_codigo': '197', 'agencia': '0001', 'conta': '1234567',
               'cnpj_titular': '00.000.000/0001-00'}
        assert resolver_conta(det, contas) == ('STONE_ASTCU', 100, 'match_completo')

    def test_match_cnpj_quando_ag_conta_nao_batem(self, contas):
        det = {'banco_codigo': '197', 'agencia': '9999', 'conta': '9999999',
               'cnpj_titular': '00.000.000/0001-00'}
        assert resolver_conta(det, contas) == ('STONE_ASTCU', 85, 'match_cnpj')

    def test_match_parcial_banco_mais_conta(self, contas):
        det = {'banco_codigo': '001', 'agencia': None, 'conta': '40102-5',
               'cnpj_titular': None}
        assert resolver_conta(det, contas) == ('BB_MN712', 70, 'match_parcial')

    def test_sem_match(self, contas):
        det = {'banco_codigo': '999', 'agencia': '1', 'conta': '1',
               'cnpj_titular': '99.999.999/9999-99'}
        assert resolver_conta(det, contas) == (None, 0, 'sem_match')

    def test_deteccao_vazia(self, contas):
        assert resolver_conta(None, contas) == (None, 0, 'sem_match')
        assert resolver_conta({}, contas) == (None, 0, 'sem_match')

    def test_contas_vazias(self):
        det = {'banco_codigo': '197', 'agencia': '0001', 'conta': '1234567'}
        assert resolver_conta(det, []) == (None, 0, 'sem_match')

    def test_ignora_conta_encerrada(self, contas):
        """Conta com ativo_ate definido não deve aparecer em match."""
        det = {'banco_codigo': '001', 'agencia': '9999', 'conta': '99999',
               'cnpj_titular': '22.222.222/0001-22'}
        assert resolver_conta(det, contas) == (None, 0, 'sem_match')


# ============================================================
# validar_cruzado — matriz de decisão (5 casos)
# ============================================================

class TestValidarCruzado:

    def test_caso1_concordam(self, contas):
        """Cabeçalho STONE_ASTCU (unit RCA) + nome sugere RCA."""
        r = validar_cruzado('STONE_ASTCU', 'RCA', contas)
        assert r['status']                    == 'concordam'
        assert r['conta_id']                  == 'STONE_ASTCU'
        assert r['unit_id']                   == 'RCA'
        assert r['confiab_rastreab']          == 'ALTA'
        assert r['metodo']                    == 'cabecalho+arquivo_concordam'
        assert r['requer_confirmacao_humana'] is False
        assert r['issue']                     is None

    def test_caso2_conflito(self, contas):
        """Cabeçalho STONE_ASTCU (unit RCA) + nome sugere MN712 → conflito."""
        r = validar_cruzado('STONE_ASTCU', 'MN712', contas)
        assert r['status']                    == 'conflito'
        assert r['conta_id']                  == 'STONE_ASTCU'
        assert r['unit_id']                   is None  # bloqueado até humano decidir
        assert r['unit_id_cabecalho']         == 'RCA'
        assert r['unit_id_nome_arquivo']      == 'MN712'
        assert r['confiab_rastreab']          == 'BAIXA'
        assert r['metodo']                    == 'conflito'
        assert r['requer_confirmacao_humana'] is True
        assert r['issue']                     == 'conflito_cabecalho_nome_arquivo'

    def test_caso3_apenas_cabecalho(self, contas):
        """Cabeçalho STONE_ASTCU, nome sem sugestão."""
        r = validar_cruzado('STONE_ASTCU', None, contas)
        assert r['status']                    == 'apenas_cabecalho'
        assert r['conta_id']                  == 'STONE_ASTCU'
        assert r['unit_id']                   == 'RCA'
        assert r['confiab_rastreab']          == 'ALTA'
        assert r['metodo']                    == 'cabecalho_sem_confirmacao_nome'
        assert r['requer_confirmacao_humana'] is False

    def test_caso4_apenas_nome(self, contas):
        """Cabeçalho não identifica, nome sugere RCA."""
        r = validar_cruzado(None, 'RCA', contas)
        assert r['status']                    == 'apenas_nome'
        assert r['conta_id']                  is None
        assert r['unit_id']                   is None
        assert r['unit_id_sugerida']          == 'RCA'
        assert r['confiab_rastreab']          == 'MEDIA'
        assert r['metodo']                    == 'apenas_nome_arquivo'
        assert r['requer_confirmacao_humana'] is True
        assert r['issue']                     == 'conta_nao_identificada_apenas_nome'

    def test_caso5_nenhum(self, contas):
        r = validar_cruzado(None, None, contas)
        assert r['status']                    == 'nenhum'
        assert r['conta_id']                  is None
        assert r['unit_id']                   is None
        assert r['confiab_rastreab']          == 'BAIXA'
        assert r['metodo']                    == 'nenhum'
        assert r['requer_confirmacao_humana'] is True
        assert r['issue']                     == 'conta_nao_identificada'


# ============================================================
# Modo legado (contas_bancarias vazio)
# ============================================================

class TestModoLegado:

    def test_modo_legado_nao_bloqueia(self):
        r = validar_cruzado(None, 'MN303', [])
        assert r['status']                    == 'modo_legado'
        assert r['unit_id']                   == 'MN303'
        assert r['confiab_rastreab']          == 'BAIXA'
        assert r['requer_confirmacao_humana'] is False
        assert r['issue']                     == 'modo_legado_ativo'

    def test_modo_legado_mesmo_sem_sugestao_de_nome(self):
        r = validar_cruzado(None, None, [])
        assert r['status']          == 'modo_legado'
        assert r['unit_id']         is None
        assert r['confiab_rastreab'] == 'BAIXA'
