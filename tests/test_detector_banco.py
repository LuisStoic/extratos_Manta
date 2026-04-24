"""
Testes do detector de cabeçalho (detectar_cabecalho / _de_linhas).

Usa a variante `detectar_cabecalho_de_linhas` para casos sintéticos,
evitando I/O de arquivos nos testes unitários. Testes de integração
com arquivos reais ficam em test_integracao_contas.py.
"""
import pytest

from detectores_banco import detectar_cabecalho_de_linhas


class TestDetectorBB:

    def test_bb_completo_com_ag_e_conta(self):
        linhas = [
            "BANCO DO BRASIL S.A.",
            "Agência: 1234-5   Conta: 98765-4",
            "CNPJ: 12.345.678/0001-90",
            "Extrato Conta Corrente - Período 01/01/2026 a 31/01/2026",
        ]
        res = detectar_cabecalho_de_linhas(linhas)
        assert res is not None
        assert res['banco_detectado'] == 'BB'
        assert res['banco_codigo']    == '001'
        assert res['agencia']         == '1234-5'
        assert res['conta']           == '98765-4'
        assert res['cnpj_titular']    == '12.345.678/0001-90'
        assert res['confianca']       == 100
        assert res['metodo']          == 'cabecalho'

    def test_bb_so_cnpj(self):
        """Cabeçalho BB com CNPJ mas sem ag/conta legível: confiança parcial."""
        linhas = ["BANCO DO BRASIL", "CNPJ 00.000.000/0001-00"]
        res = detectar_cabecalho_de_linhas(linhas)
        assert res['banco_detectado'] == 'BB'
        assert res['agencia'] is None and res['conta'] is None
        assert res['cnpj_titular'] == '00.000.000/0001-00'
        assert res['confianca'] == 70


class TestDetectorStone:

    def test_stone_completo(self):
        linhas = [
            "Stone Pagamentos S.A.",
            "Agência: 0001  Conta: 1234567",
            "CNPJ: 00.000.000/0001-00",
        ]
        res = detectar_cabecalho_de_linhas(linhas)
        assert res['banco_detectado'] == 'Stone'
        assert res['banco_codigo']    == '197'
        assert res['conta']           == '1234567'
        assert res['confianca']       == 100

    def test_stone_sem_conta(self):
        """Stone identificado mas sem linha de conta explícita."""
        linhas = ["Stone Pagamentos S.A.", "Outras informações…"]
        res = detectar_cabecalho_de_linhas(linhas)
        assert res['banco_detectado'] == 'Stone'
        assert res['conta'] is None
        assert res['confianca'] == 60


class TestDetectorCora:

    def test_cora_identificado_parcial(self):
        linhas = ["Cora SCD", "Extrato bancário", "CNPJ: 11.222.333/0001-44"]
        res = detectar_cabecalho_de_linhas(linhas)
        assert res['banco_detectado'] == 'Cora'
        assert res['conta'] is None     # TODO v9.1
        assert res['cnpj_titular'] == '11.222.333/0001-44'


class TestDetectorBRB:

    def test_brb_identificado_parcial(self):
        linhas = ["Banco de Brasília S.A. (BRB)", "Agência 100 Conta 12345"]
        res = detectar_cabecalho_de_linhas(linhas)
        assert res['banco_detectado'] == 'BRB'


class TestSemIdentificacao:

    def test_arquivo_sem_marca_de_banco(self):
        linhas = ["Planilha genérica", "Coluna A, Coluna B", "123,45"]
        assert detectar_cabecalho_de_linhas(linhas) is None

    def test_lista_vazia(self):
        assert detectar_cabecalho_de_linhas([]) is None

    def test_none_defensive(self):
        """Uma linha solta não deve explodir."""
        assert detectar_cabecalho_de_linhas(["qualquer texto aleatório"]) is None


class TestPrioridadeEntreDetectores:

    def test_stone_ganha_de_bb_quando_explicito(self):
        """Arquivo Stone pode citar 'Banco' em boilerplate; a ordem força Stone."""
        linhas = [
            "Stone Pagamentos S.A. — instituição de pagamento",
            "O Banco do Beneficiário consta na linha abaixo.",
            "Conta: 7654321",
        ]
        res = detectar_cabecalho_de_linhas(linhas)
        assert res['banco_detectado'] == 'Stone'
