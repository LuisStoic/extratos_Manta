"""
Detectores de cabeçalho de arquivos de extrato bancário.

Função pública:
    detectar_cabecalho(filepath, filename) -> dict | None

Extrai banco, agência, conta, CNPJ do titular e período a partir das
primeiras linhas do arquivo. O cabeçalho é a FONTE PRIMÁRIA DE VERDADE
para atribuição de conta; o nome do arquivo é apenas controle secundário.

Cada detector específico recebe `linhas` (lista de strings das primeiras
~50 linhas) e retorna um dict parcial ou None.

Detectores implementados: BB, Stone.
TODO(v9.1): Cora (identificar pelo padrão de header CSV com '"Data"'),
            BRB (procurar 'BRB' em linhas 1-10; mapear agência por CNPJ).
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

import pandas as pd


# ============================================================
# Helpers de leitura
# ============================================================

_MAX_HEAD_LINES = 50


def _ler_primeiras_linhas(filepath: str, filename: str) -> list[str]:
    """Retorna lista de strings normalizadas das primeiras linhas do arquivo.

    Pandas é usado para ler cabeçalhos de xlsx/xls/csv como texto bruto.
    Cada linha é a concatenação das células não-vazias, separadas por
    espaço duplo (preserva ordem e permite regex simples).
    """
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    try:
        if ext in ('xlsx', 'xls'):
            df = pd.read_excel(filepath, dtype=str, header=None, nrows=_MAX_HEAD_LINES)
        elif ext == 'csv':
            # CSVs bancários variam em encoding; tenta utf-8 e latin-1.
            for enc in ('utf-8', 'latin-1', 'cp1252'):
                try:
                    df = pd.read_csv(filepath, encoding=enc, dtype=str, header=None,
                                     nrows=_MAX_HEAD_LINES, on_bad_lines='skip',
                                     sep=None, engine='python')
                    break
                except Exception:
                    continue
            else:
                return []
        else:
            # OFX e PDF não passam por este detector (OFX tem estrutura própria,
            # PDF é tratado pelo pdf_extractor que já retorna DataFrame plano).
            return []
    except Exception:
        return []

    linhas: list[str] = []
    for _, row in df.iterrows():
        celulas = [str(c).strip() for c in row.tolist()
                   if c is not None and str(c).strip() and str(c).strip().lower() != 'nan']
        if celulas:
            linhas.append('  '.join(celulas))
    return linhas


# ============================================================
# Detector Banco do Brasil (BB)
# ============================================================

_RX_BB_IDENT = re.compile(r'BANCO\s+DO\s+BRASIL|\bBB\s+S\.?A\.?', re.IGNORECASE)
_RX_BB_AGENCIA = re.compile(
    r'(?:Ag[êe]ncia|AG|Ag\.?)\s*[:\-\s]\s*(\d{3,5}(?:[-\.]\s?[\dX])?)',
    re.IGNORECASE,
)
_RX_BB_CONTA = re.compile(
    r'(?:Conta(?:\s+Corrente)?|C/C|CC)\s*[:\-\s]\s*(\d{4,10}(?:[-\.]\s?[\dX])?)',
    re.IGNORECASE,
)
_RX_CNPJ = re.compile(r'\b(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})\b')


def _detectar_bb(linhas: list[str]) -> dict | None:
    texto = '\n'.join(linhas)
    if not _RX_BB_IDENT.search(texto):
        return None

    agencia = conta = cnpj = None
    m = _RX_BB_AGENCIA.search(texto)
    if m:
        agencia = m.group(1).replace(' ', '').replace('.', '-')
    m = _RX_BB_CONTA.search(texto)
    if m:
        conta = m.group(1).replace(' ', '').replace('.', '-')
    m = _RX_CNPJ.search(texto)
    if m:
        cnpj = m.group(1)

    confianca = 100 if (agencia and conta) else (70 if (agencia or conta or cnpj) else 50)
    return {
        'banco_detectado': 'BB',
        'banco_codigo':    '001',
        'agencia':         agencia,
        'conta':           conta,
        'cnpj_titular':    cnpj,
        'periodo_inicio':  None,
        'periodo_fim':     None,
        'saldo_inicial':   None,
        'saldo_final':     None,
        'confianca':       confianca,
        'metodo':          'cabecalho',
    }


# ============================================================
# Detector Stone
# ============================================================

_RX_STONE_IDENT = re.compile(r'\bStone\s+Pagamentos\b|\bSTONE\b', re.IGNORECASE)
_RX_STONE_CONTA = re.compile(r'Conta\s*[:\-]\s*(\d{4,12})', re.IGNORECASE)
_RX_STONE_AGENCIA = re.compile(r'(?:Ag[êe]ncia|AG)\s*[:\-]\s*(\d{3,5})', re.IGNORECASE)


def _detectar_stone(linhas: list[str]) -> dict | None:
    texto = '\n'.join(linhas)
    if not _RX_STONE_IDENT.search(texto):
        return None

    agencia = conta = cnpj = None
    m = _RX_STONE_AGENCIA.search(texto)
    if m:
        agencia = m.group(1)
    m = _RX_STONE_CONTA.search(texto)
    if m:
        conta = m.group(1)
    m = _RX_CNPJ.search(texto)
    if m:
        cnpj = m.group(1)

    confianca = 100 if conta else (70 if cnpj else 60)
    return {
        'banco_detectado': 'Stone',
        'banco_codigo':    '197',
        'agencia':         agencia,
        'conta':           conta,
        'cnpj_titular':    cnpj,
        'periodo_inicio':  None,
        'periodo_fim':     None,
        'saldo_inicial':   None,
        'saldo_final':     None,
        'confianca':       confianca,
        'metodo':          'cabecalho',
    }


# ============================================================
# Detectores parciais (identificam o banco, não extraem detalhes)
# ============================================================

_RX_CORA_IDENT = re.compile(r'\bCora\s*(SCD|S\.A\.|SA)?\b', re.IGNORECASE)
_RX_BRB_IDENT  = re.compile(r'\bBRB\b|\bBanco\s+de\s+Bras[ií]lia\b', re.IGNORECASE)


def _detectar_cora(linhas: list[str]) -> dict | None:
    texto = '\n'.join(linhas)
    if not _RX_CORA_IDENT.search(texto):
        return None
    m = _RX_CNPJ.search(texto)
    # TODO(v9.1): Cora CSV costuma ter 'Conta corrente:' em uma linha separada.
    # Por ora, registra o banco mas deixa conta/agência para preenchimento manual.
    return {
        'banco_detectado': 'Cora',
        'banco_codigo':    '403',
        'agencia':         None,
        'conta':           None,
        'cnpj_titular':    m.group(1) if m else None,
        'periodo_inicio':  None,
        'periodo_fim':     None,
        'saldo_inicial':   None,
        'saldo_final':     None,
        'confianca':       40,
        'metodo':          'cabecalho',
    }


def _detectar_brb(linhas: list[str]) -> dict | None:
    texto = '\n'.join(linhas)
    if not _RX_BRB_IDENT.search(texto):
        return None
    m = _RX_CNPJ.search(texto)
    # TODO(v9.1): BRB legacy XLS tem agência/conta em posição fixa (linha 3-4);
    # o formato web-banking tem em cabeçalho textual. Requer cases reais.
    return {
        'banco_detectado': 'BRB',
        'banco_codigo':    '070',
        'agencia':         None,
        'conta':           None,
        'cnpj_titular':    m.group(1) if m else None,
        'periodo_inicio':  None,
        'periodo_fim':     None,
        'saldo_inicial':   None,
        'saldo_final':     None,
        'confianca':       40,
        'metodo':          'cabecalho',
    }


# ============================================================
# Entry point
# ============================================================

# Ordem: detectores mais específicos primeiro. Stone antes de BB pois 'Stone
# Pagamentos' pode incidentalmente citar 'Banco' em boilerplate.
_DETECTORES = (_detectar_stone, _detectar_bb, _detectar_cora, _detectar_brb)


def detectar_cabecalho(filepath: str, filename: str) -> dict | None:
    """Retorna dict de identificação ou None se nenhum banco foi reconhecido.

    Nunca inventa valores. Campos não identificados ficam None. Cabe ao
    `resolver_conta` decidir o que fazer com identificação parcial.
    """
    linhas = _ler_primeiras_linhas(filepath, filename)
    if not linhas:
        return None

    for det in _DETECTORES:
        resultado = det(linhas)
        if resultado is not None:
            return resultado
    return None


def detectar_cabecalho_de_linhas(linhas: list[str]) -> dict | None:
    """Variante para testes: aceita as linhas já extraídas, sem I/O de arquivo."""
    if not linhas:
        return None
    for det in _DETECTORES:
        resultado = det(linhas)
        if resultado is not None:
            return resultado
    return None
