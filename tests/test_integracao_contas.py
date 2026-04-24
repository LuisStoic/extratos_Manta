"""
Integração do fluxo v9: detecção por cabeçalho × nome do arquivo,
regressão do bug RRS, processamento em modo_legado e conflito resolvido
manualmente.

Os testes geram arquivos XLSX sintéticos em tmp_path para exercitar
processar() end-to-end sem depender de extratos reais.
"""
from pathlib import Path
import hashlib
import json

import pandas as pd
import pytest

import app as appmod
from detectores_banco import detectar_cabecalho
from resolucao_conta  import resolver_conta, validar_cruzado


# ============================================================
# Helpers
# ============================================================

def _md5(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def _escrever_xlsx_bb_rural(path: Path):
    """Extrato BB estilo conta rural (RRS = 'Rural Rio Sul' fictício).

    Cabeçalho identifica BB + ag/conta reais. Nome do arquivo no upload
    tem 'RRS' — que o fuzzy de encontrar_unidade pode mapear fracamente
    para qualquer unidade cadastrada (ex: RCA se existir). O cabeçalho
    DEVE prevalecer; o sistema NÃO pode atribuir RCA automaticamente.
    """
    header_info = pd.DataFrame([
        ['BANCO DO BRASIL S.A.'],
        ['Agência: 3791-1   Conta: 40102-5'],
        ['CNPJ: 11.111.111/0001-11'],
        ['Extrato Conta Corrente — Período 01/03/2026 a 31/03/2026'],
        [''],
    ])
    transacoes = pd.DataFrame([
        ['Data', 'Histórico', 'Débito', 'Crédito'],
        ['15/03/2026', 'FINANC AGROPEC 123', '', '10000,00'],
        ['18/03/2026', 'PAGTO FORNECEDOR', '3500,00', ''],
    ])
    with pd.ExcelWriter(path, engine='openpyxl') as xw:
        header_info.to_excel(xw, index=False, header=False, startrow=0, sheet_name='Sheet1')
        transacoes.to_excel(xw, index=False, header=False, startrow=5, sheet_name='Sheet1')


def _escrever_xlsx_stone(path: Path):
    """Extrato Stone com cabeçalho completo."""
    header = pd.DataFrame([
        ['Stone Pagamentos S.A.'],
        ['Agência: 0001   Conta: 1234567'],
        ['CNPJ: 00.000.000/0001-00'],
        ['Extrato Conta Pagamento'],
        [''],
    ])
    tx = pd.DataFrame([
        ['Data',       'Descricao',          'Valor'],
        ['10/03/2026', 'TED recebida',       '2000,00'],
        ['12/03/2026', 'Pagamento cartão',   '-500,00'],
    ])
    with pd.ExcelWriter(path, engine='openpyxl') as xw:
        header.to_excel(xw, index=False, header=False, startrow=0, sheet_name='Sheet1')
        tx.to_excel(xw, index=False, header=False, startrow=5, sheet_name='Sheet1')


def _escrever_xlsx_generico_modo_legado(path: Path):
    """Planilha sem marca de banco — usado para testar modo_legado."""
    tx = pd.DataFrame([
        ['Data',       'Historico',     'Valor'],
        ['05/03/2026', 'Transferência', '150,00'],
        ['06/03/2026', 'Pagamento',     '-50,00'],
    ])
    tx.to_excel(path, index=False, header=False)


def _registrar_arquivo(path: Path):
    appmod.SESSION['arquivos'].append({
        'filename': path.name,
        'size':     path.stat().st_size,
        'path':     str(path),
        'hash':     _md5(path),
    })


def _resetar_sessao():
    appmod.SESSION['arquivos']               = []
    appmod.SESSION['lancamentos']            = []
    appmod.SESSION['schema_map']             = {}
    appmod.SESSION['processado']             = False
    appmod.SESSION['doc_verificados']        = {}
    appmod.SESSION['previews']               = {}
    appmod.SESSION['deteccao_cab']           = {}
    appmod.SESSION['cruzamento']             = {}
    appmod.SESSION['conta_por_arquivo']      = {}
    appmod.SESSION['conta_por_arquivo_meta'] = {}
    appmod.SESSION['arquivos_bloqueados']    = []
    appmod.SESSION['audit_log']              = []
    appmod.SESSION['progresso']              = {'pct': 0, 'msg': '', 'ativo': False}


@pytest.fixture
def isolar_cfg(monkeypatch):
    """Substitui CFG por cópia isolada — nunca persiste em config.json real."""
    from copy import deepcopy
    orig = deepcopy(appmod.CFG)
    monkeypatch.setattr(appmod, 'CFG', deepcopy(orig))
    monkeypatch.setattr(appmod, 'save_config', lambda _cfg: None)
    _resetar_sessao()
    yield appmod.CFG
    _resetar_sessao()


# ============================================================
# Regressão do bug original (RRS → RaiaClube ASTCU)
# ============================================================

class TestRegressaoRRS:
    """Arquivo BB renomeado com 'RRS' no nome não pode virar conta Stone/RCA.

    Cenário: operador fez upload de um extrato BB (conta agropecuária real)
    mas o nome do arquivo contém 'RRS' e o fuzzy bate acidentalmente acima
    de 80% com alguma unidade RCA (ex.: 'RRS' ≈ 'RCA' por coincidência).
    O cabeçalho IDENTIFICA BB corretamente; resolver_conta devolve BB_MN712
    (cadastrada na tabela-mãe). validar_cruzado DEVE detectar conflito e
    NUNCA atribuir RCA sem confirmação humana explícita.
    """

    def test_cabecalho_bb_ganha_sobre_nome_rrs(self, tmp_path, isolar_cfg):
        # Contas cadastradas: BB_MN712 (real) + STONE_ASTCU (isca para confusão).
        isolar_cfg['contas_bancarias'] = [
            {'id': 'BB_MN712', 'banco_nome': 'BB', 'banco_codigo': '001',
             'agencia': '3791-1', 'conta': '40102-5',
             'cnpj_titular': '11.111.111/0001-11', 'unit_id': 'MN712',
             'ativo_desde': '2025-01-01', 'ativo_ate': None},
            {'id': 'STONE_ASTCU', 'banco_nome': 'Stone', 'banco_codigo': '197',
             'agencia': '0001', 'conta': '1234567',
             'cnpj_titular': '00.000.000/0001-00', 'unit_id': 'RCA',
             'ativo_desde': '2025-01-01', 'ativo_ate': None},
        ]

        arq = tmp_path / 'EXTRATO_RRS_AGRONEGOCIO_2026_03.xlsx'
        _escrever_xlsx_bb_rural(arq)

        deteccao = detectar_cabecalho(str(arq), arq.name)
        assert deteccao is not None
        assert deteccao['banco_detectado'] == 'BB'
        assert deteccao['agencia'] == '3791-1'
        assert deteccao['conta']   == '40102-5'

        conta_id_cab, conf, _ = resolver_conta(deteccao, isolar_cfg['contas_bancarias'])
        assert conta_id_cab == 'BB_MN712'
        assert conf == 100

        uid_nome, conf_nome, _ = appmod.encontrar_unidade(arq.name)
        unit_id_nome = uid_nome if conf_nome >= 80 else None

        cruz = validar_cruzado(conta_id_cab, unit_id_nome, isolar_cfg['contas_bancarias'])
        # Independentemente do que o fuzzy mandou, a conta é BB_MN712 (MN712),
        # nunca RCA sem confirmação humana explícita.
        assert cruz['conta_id'] == 'BB_MN712'
        assert cruz['unit_id_cabecalho'] == 'MN712'
        if cruz['status'] == 'conflito':
            assert cruz['requer_confirmacao_humana'] is True
            assert cruz['issue'] == 'conflito_cabecalho_nome_arquivo'
            assert cruz['unit_id'] is None
        else:
            # apenas_cabecalho — fuzzy ficou abaixo do limiar
            assert cruz['status'] == 'apenas_cabecalho'
            assert cruz['unit_id'] == 'MN712'


# ============================================================
# processar() — cenários end-to-end
# ============================================================

class TestProcessarFluxoV9:

    def test_concordam_processa_com_rastreab_alta(self, tmp_path, isolar_cfg):
        isolar_cfg['contas_bancarias'] = [
            {'id': 'STONE_ASTCU', 'banco_nome': 'Stone', 'banco_codigo': '197',
             'agencia': '0001', 'conta': '1234567',
             'cnpj_titular': '00.000.000/0001-00', 'unit_id': 'RCA',
             'ativo_desde': '2025-01-01', 'ativo_ate': None},
        ]
        # Nome do arquivo casa com RCA via 'ASTCU'.
        arq = tmp_path / 'Extrato_ASTCU_marco2026.xlsx'
        _escrever_xlsx_stone(arq)
        _registrar_arquivo(arq)

        app_test = appmod.app.test_client()
        resp = app_test.post('/api/processar')
        data = resp.get_json()
        assert data['ok'] is True
        assert data['total'] > 0
        assert 'bloqueados' not in data

        lancs = appmod.SESSION['lancamentos']
        assert lancs, 'deveria ter produzido lançamentos'
        for l in lancs:
            assert l['conta_id'] == 'STONE_ASTCU'
            assert l['Confiab_Rastreabilidade'] == 'ALTA'
            assert l['metodo_atribuicao'] == 'cabecalho+arquivo_concordam'
            assert l['unidade_id'] == 'RCA'

    def test_modo_legado_processa_com_rastreab_baixa(self, tmp_path, isolar_cfg):
        isolar_cfg['contas_bancarias'] = []  # modo_legado
        arq = tmp_path / 'extrato_generico_202603.xlsx'
        _escrever_xlsx_generico_modo_legado(arq)
        _registrar_arquivo(arq)

        app_test = appmod.app.test_client()
        resp = app_test.post('/api/processar')
        data = resp.get_json()
        assert data['ok'] is True
        lancs = appmod.SESSION['lancamentos']
        assert lancs
        for l in lancs:
            assert l['Confiab_Rastreabilidade'] == 'BAIXA'
            assert l['metodo_atribuicao'] == 'modo_legado'
            assert l['conta_id'] is None

    def test_conflito_sem_confirmacao_bloqueia_arquivo(self, tmp_path, isolar_cfg):
        """Arquivo BB com nome forçando unit_id diferente: bloqueia."""
        isolar_cfg['contas_bancarias'] = [
            {'id': 'BB_MN712', 'banco_nome': 'BB', 'banco_codigo': '001',
             'agencia': '3791-1', 'conta': '40102-5',
             'cnpj_titular': '11.111.111/0001-11', 'unit_id': 'MN712',
             'ativo_desde': '2025-01-01', 'ativo_ate': None},
        ]
        # Nome do arquivo contém 'MANTA-VP' — fuzzy bate forte com unit MNVP.
        arq = tmp_path / 'MANTA-VP_2026_MARCO.xlsx'
        _escrever_xlsx_bb_rural(arq)
        _registrar_arquivo(arq)

        app_test = appmod.app.test_client()
        resp = app_test.post('/api/processar')
        data = resp.get_json()
        assert data['ok'] is True
        # Conflito deve ter bloqueado.
        assert 'bloqueados' in data
        nomes = [b['filename'] for b in data['bloqueados']]
        assert arq.name in nomes
        # Nenhum lançamento com unit MNVP (erro que queremos evitar) —
        # na verdade nenhum lançamento deste arquivo entra na lista.
        for l in appmod.SESSION['lancamentos']:
            assert l['arquivo'] != arq.name

    def test_conflito_com_confirmacao_manual_processa_com_media(self, tmp_path, isolar_cfg):
        """Operador escolhe conta manualmente → processa com Rastreab=MEDIA + audit."""
        isolar_cfg['contas_bancarias'] = [
            {'id': 'BB_MN712', 'banco_nome': 'BB', 'banco_codigo': '001',
             'agencia': '3791-1', 'conta': '40102-5',
             'cnpj_titular': '11.111.111/0001-11', 'unit_id': 'MN712',
             'ativo_desde': '2025-01-01', 'ativo_ate': None},
        ]
        arq = tmp_path / 'MANTA-VP_2026_MARCO.xlsx'
        _escrever_xlsx_bb_rural(arq)
        _registrar_arquivo(arq)

        # Operador confirmou manualmente a conta correta (a do cabeçalho).
        appmod.SESSION['conta_por_arquivo'][arq.name] = 'BB_MN712'
        appmod.SESSION['conta_por_arquivo_meta'][arq.name] = {
            'metodo_original': 'conflito',
            'escolha_humana':  'BB_MN712',
            'motivo':          'Confirmado após verificar CNPJ no extrato',
            'timestamp':       '2026-04-24T10:00:00',
        }

        app_test = appmod.app.test_client()
        resp = app_test.post('/api/processar')
        data = resp.get_json()
        assert data['ok'] is True
        lancs = [l for l in appmod.SESSION['lancamentos'] if l['arquivo'] == arq.name]
        assert lancs
        for l in lancs:
            assert l['conta_id'] == 'BB_MN712'
            assert l['Confiab_Rastreabilidade'] == 'MEDIA'
            assert l['metodo_atribuicao'].startswith('conflito_confirmado_manual')

        # Audit log registrou a decisão.
        audit = appmod.SESSION['audit_log']
        assert any(a['filename'] == arq.name and
                   a['acao'] == 'conflito_resolvido_manualmente'
                   for a in audit)
