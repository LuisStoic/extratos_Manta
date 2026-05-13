"""
Resolução e validação cruzada de conta bancária.

Duas funções públicas:

  resolver_conta(deteccao, contas_bancarias) -> (conta_id, confianca, motivo)
    Procura uma conta cadastrada a partir do que o detector de cabeçalho
    extraiu. Match completo > match CNPJ > match parcial.

  validar_cruzado(conta_id_cabecalho, unit_id_nome_arquivo, contas_bancarias) -> dict
    Implementa a matriz de decisão cabeçalho × nome_arquivo (5 casos).
    Princípio inegociável: o cabeçalho é a fonte primária; o nome do
    arquivo só confirma ou questiona. Nome do arquivo sozinho NUNCA
    atribui conta automaticamente.
"""
from __future__ import annotations


# ============================================================
# Normalizadores
# ============================================================

def _norm(s: str | None) -> str:
    """Normaliza string removendo não-alfanuméricos e passando para upper."""
    if not s:
        return ''
    return ''.join(ch for ch in str(s).upper() if ch.isalnum())


def _conta_ativa(c: dict) -> bool:
    """Ignora contas encerradas. Para este v9, trata 'ativo_ate' None como ativa."""
    return c.get('ativo_ate') in (None, '', 'null')


# ============================================================
# resolver_conta
# ============================================================

def resolver_conta(deteccao: dict | None,
                   contas_bancarias: list[dict]) -> tuple[str | None, int, str]:
    """Escolhe a conta cadastrada que bate com o cabeçalho detectado.

    Ordem de preferência (do mais específico para o menos):
      1. banco_codigo + agencia + conta → confianca 100, 'match_completo'
      2. cnpj_titular                   → confianca  85, 'match_cnpj'
      3. banco_codigo + conta           → confianca  70, 'match_parcial'
      4. nenhum match                   → (None, 0, 'sem_match')

    Nunca usa nome do arquivo.
    """
    if not deteccao or not contas_bancarias:
        return None, 0, 'sem_match'

    ativas = [c for c in contas_bancarias if _conta_ativa(c)]
    if not ativas:
        return None, 0, 'sem_match'

    banco_cod = _norm(deteccao.get('banco_codigo'))
    ag        = _norm(deteccao.get('agencia'))
    cc        = _norm(deteccao.get('conta'))
    cnpj      = _norm(deteccao.get('cnpj_titular'))

    # 1. Match completo
    if banco_cod and ag and cc:
        for c in ativas:
            if (_norm(c.get('banco_codigo')) == banco_cod and
                _norm(c.get('agencia'))      == ag and
                _norm(c.get('conta'))        == cc):
                return c['id'], 100, 'match_completo'

    # 2. Match por CNPJ
    if cnpj:
        for c in ativas:
            if _norm(c.get('cnpj_titular')) == cnpj:
                return c['id'], 85, 'match_cnpj'

    # 3. Match parcial (banco + conta, sem agência)
    if banco_cod and cc:
        for c in ativas:
            if (_norm(c.get('banco_codigo')) == banco_cod and
                _norm(c.get('conta'))        == cc):
                return c['id'], 70, 'match_parcial'

    return None, 0, 'sem_match'


# ============================================================
# validar_cruzado
# ============================================================

def _unit_de_conta(conta_id: str | None, contas_bancarias: list[dict]) -> str | None:
    if not conta_id:
        return None
    for c in contas_bancarias:
        if c.get('id') == conta_id:
            return c.get('unit_id')
    return None


def validar_cruzado(conta_id_cabecalho: str | None,
                    unit_id_nome_arquivo: str | None,
                    contas_bancarias: list[dict]) -> dict:
    """Cruza identificação por cabeçalho × nome do arquivo.

    Os 5 casos da matriz de decisão estão documentados no briefing v9.
    Retorna um dict com chaves estáveis para consumo pela UI e pelo
    pipeline em `processar()`. Nunca resolve conflito automaticamente.

    Modo legado: se `contas_bancarias` está vazio (primeira execução ou
    ambiente ainda não migrado), nenhum fluxo de bloqueio é ativado.
    Todos os arquivos caem em 'modo_legado', processam com
    Rastreab='BAIXA' e o sistema se comporta como v8.
    """
    # Modo legado — ativa quando a tabela-mãe ainda não foi populada.
    if not contas_bancarias:
        return {
            'status':                    'modo_legado',
            'conta_id':                  None,
            'unit_id':                   unit_id_nome_arquivo,
            'unit_id_cabecalho':         None,
            'unit_id_nome_arquivo':      unit_id_nome_arquivo,
            'confiab_rastreab':          'BAIXA',
            'metodo':                    'modo_legado',
            'requer_confirmacao_humana': False,
            'issue':                     'modo_legado_ativo',
        }

    unit_cab = _unit_de_conta(conta_id_cabecalho, contas_bancarias)
    tem_cab  = conta_id_cabecalho is not None
    tem_nome = unit_id_nome_arquivo is not None

    # CASO 5 — nenhum método identificou
    if not tem_cab and not tem_nome:
        return {
            'status':                    'nenhum',
            'conta_id':                  None,
            'unit_id':                   None,
            'unit_id_cabecalho':         None,
            'unit_id_nome_arquivo':      None,
            'confiab_rastreab':          'BAIXA',
            'metodo':                    'nenhum',
            'requer_confirmacao_humana': True,
            'issue':                     'conta_nao_identificada',
        }

    # CASO 4 — só nome do arquivo sugere algo
    if not tem_cab and tem_nome:
        return {
            'status':                    'apenas_nome',
            'conta_id':                  None,
            'unit_id':                   None,            # não atribui automaticamente
            'unit_id_cabecalho':         None,
            'unit_id_nome_arquivo':      unit_id_nome_arquivo,
            'unit_id_sugerida':          unit_id_nome_arquivo,
            'confiab_rastreab':          'MEDIA',
            'metodo':                    'apenas_nome_arquivo',
            'requer_confirmacao_humana': True,
            'issue':                     'conta_nao_identificada_apenas_nome',
        }

    # CASO 3 — só o cabeçalho identificou (nome do arquivo sem sugestão)
    if tem_cab and not tem_nome:
        return {
            'status':                    'apenas_cabecalho',
            'conta_id':                  conta_id_cabecalho,
            'unit_id':                   unit_cab,
            'unit_id_cabecalho':         unit_cab,
            'unit_id_nome_arquivo':      None,
            'confiab_rastreab':          'ALTA',
            'metodo':                    'cabecalho_sem_confirmacao_nome',
            'requer_confirmacao_humana': False,
            'issue':                     None,
        }

    # Ambos identificaram: concordam ou conflito?
    if unit_cab == unit_id_nome_arquivo:
        # CASO 1 — concordam
        return {
            'status':                    'concordam',
            'conta_id':                  conta_id_cabecalho,
            'unit_id':                   unit_cab,
            'unit_id_cabecalho':         unit_cab,
            'unit_id_nome_arquivo':      unit_id_nome_arquivo,
            'confiab_rastreab':          'ALTA',
            'metodo':                    'cabecalho+arquivo_concordam',
            'requer_confirmacao_humana': False,
            'issue':                     None,
        }

    # CASO 2 — conflito
    return {
        'status':                    'conflito',
        'conta_id':                  conta_id_cabecalho,
        'unit_id':                   None,  # bloqueado até confirmação humana
        'unit_id_cabecalho':         unit_cab,
        'unit_id_nome_arquivo':      unit_id_nome_arquivo,
        'confiab_rastreab':          'BAIXA',
        'metodo':                    'conflito',
        'requer_confirmacao_humana': True,
        'issue':                     'conflito_cabecalho_nome_arquivo',
    }
