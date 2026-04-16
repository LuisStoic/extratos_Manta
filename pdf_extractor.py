"""
================================================================================
PDF EXTRACTOR — Pipeline em cascata com fallback e error handling
Stoic Capital
================================================================================

Estratégia:
  [1] pdfplumber lattice  → tabelas com bordas
  [2] pdfplumber stream   → tabelas sem bordas (heurística por espaços)
  [3] pymupdf (fitz)      → extração por blocos/linhas
  [4] OCR (pytesseract)   → opcional, para PDFs escaneados
  [5] Quarentena          → registra falha, não derruba o pipeline

Cada extrator retorna (DataFrame, score, warnings). O orquestrador escolhe
o melhor resultado por página e concatena. Erros são contidos por página,
nunca derrubando o arquivo inteiro.

Saída: DataFrame compatível com ler_df() do app.py + lista de warnings.
"""

from __future__ import annotations
import re
from pathlib import Path
from typing import Optional
import pandas as pd

# ── Imports opcionais (graceful degradation) ─────────────────────────
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    import fitz  # pymupdf
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

try:
    import pytesseract
    from pdf2image import convert_from_path
    HAS_OCR = True
except ImportError:
    HAS_OCR = False


# ── Configuração de qualidade ────────────────────────────────────────
SCORE_MINIMO = 0.55          # abaixo disso, tenta próximo extrator
HEADER_REGEX = re.compile(
    r'data|hist[óo]rico|descri[çc][ãa]o|valor|saldo|d[ée]bito|cr[ée]dito|lan[çc]amento',
    re.IGNORECASE,
)


# ============================================================
# SCORE DE QUALIDADE
# ============================================================

def avaliar_qualidade(df: pd.DataFrame) -> float:
    """
    Score 0.0–1.0 baseado em heurísticas baratas:
      - tem cabeçalhos esperados?
      - % de células não-vazias
      - consistência de nº de colunas
      - presença de coluna parseável como data
    """
    if df is None or df.empty or len(df.columns) < 2:
        return 0.0

    score = 0.0

    # 1. Cabeçalhos esperados (peso 0.30)
    cols_str = ' '.join(str(c) for c in df.columns)
    if HEADER_REGEX.search(cols_str):
        score += 0.30

    # 2. Densidade de células preenchidas (peso 0.25)
    total = df.size
    nao_vazias = df.notna().sum().sum() - (df == '').sum().sum()
    densidade = nao_vazias / total if total else 0
    score += 0.25 * min(densidade, 1.0)

    # 3. Tem alguma coluna que parece data (peso 0.25)
    for col in df.columns:
        amostra = df[col].dropna().astype(str).head(10)
        hits = sum(1 for v in amostra
                   if re.search(r'\d{1,2}[/\-.]\d{1,2}[/\-.]\d{2,4}', v))
        if hits >= 3:
            score += 0.25
            break

    # 4. Tem alguma coluna que parece valor monetário (peso 0.20)
    for col in df.columns:
        amostra = df[col].dropna().astype(str).head(10)
        hits = sum(1 for v in amostra
                   if re.search(r'-?[\d.,]+\d{2}', v) and any(c.isdigit() for c in v))
        if hits >= 3:
            score += 0.20
            break

    return min(score, 1.0)


# ============================================================
# EXTRATOR 1+2 — pdfplumber (lattice + stream)
# ============================================================

def extrair_pdfplumber(filepath: str, modo: str = 'lattice'):
    """
    modo: 'lattice' (linhas explícitas) ou 'stream' (texto/espaços).
    Retorna lista de (pagina, df, score, warnings).
    """
    if not HAS_PDFPLUMBER:
        return []

    settings = (
        {'vertical_strategy': 'lines', 'horizontal_strategy': 'lines'}
        if modo == 'lattice'
        else {'vertical_strategy': 'text', 'horizontal_strategy': 'text',
              'snap_tolerance': 4}
    )

    resultados = []
    try:
        with pdfplumber.open(filepath) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                warnings = []
                try:
                    tabelas = page.extract_tables(table_settings=settings) or []
                    if not tabelas:
                        resultados.append((i, None, 0.0,
                                           [f'pdfplumber/{modo}: nenhuma tabela na pág {i}']))
                        continue

                    # Pega a maior tabela da página
                    maior = max(tabelas, key=lambda t: len(t) * (len(t[0]) if t else 0))
                    if len(maior) < 2:
                        resultados.append((i, None, 0.0,
                                           [f'pdfplumber/{modo}: tabela < 2 linhas pág {i}']))
                        continue

                    df = pd.DataFrame(maior[1:], columns=[
                        str(c).strip() if c else f'col_{idx}'
                        for idx, c in enumerate(maior[0])
                    ])
                    df = df.fillna('').astype(str)
                    score = avaliar_qualidade(df)
                    resultados.append((i, df, score, warnings))
                except Exception as e:
                    resultados.append((i, None, 0.0,
                                       [f'pdfplumber/{modo} erro pág {i}: {e}']))
    except Exception as e:
        return [(0, None, 0.0, [f'pdfplumber/{modo} falhou ao abrir: {e}'])]

    return resultados


# ============================================================
# EXTRATOR 3 — pymupdf (fitz) por blocos
# ============================================================

def extrair_pymupdf(filepath: str):
    """
    Fallback: extrai linhas de texto e tenta inferir colunas por
    posição X dos blocos.
    """
    if not HAS_PYMUPDF:
        return []

    resultados = []
    try:
        doc = fitz.open(filepath)
        for i, page in enumerate(doc, start=1):
            warnings = []
            try:
                blocks = page.get_text('blocks') or []
                # Agrupa blocos por linha (Y aproximado)
                linhas = {}
                for b in blocks:
                    if len(b) < 5:
                        continue
                    x0, y0, x1, y1, txt = b[0], b[1], b[2], b[3], b[4]
                    chave = round(y0 / 5) * 5
                    linhas.setdefault(chave, []).append((x0, txt.strip()))

                if not linhas:
                    resultados.append((i, None, 0.0,
                                       [f'pymupdf: pág {i} sem texto']))
                    continue

                rows = []
                for y in sorted(linhas.keys()):
                    cells = [t for _, t in sorted(linhas[y])]
                    if any(cells):
                        rows.append(cells)

                if len(rows) < 2:
                    resultados.append((i, None, 0.0,
                                       [f'pymupdf: pág {i} insuficiente']))
                    continue

                # Normaliza nº de colunas
                ncols = max(len(r) for r in rows)
                rows = [r + [''] * (ncols - len(r)) for r in rows]
                df = pd.DataFrame(rows[1:], columns=rows[0]).fillna('').astype(str)
                score = avaliar_qualidade(df)
                resultados.append((i, df, score, warnings))
            except Exception as e:
                resultados.append((i, None, 0.0,
                                   [f'pymupdf erro pág {i}: {e}']))
        doc.close()
    except Exception as e:
        return [(0, None, 0.0, [f'pymupdf falhou ao abrir: {e}'])]

    return resultados


# ============================================================
# EXTRATOR 4 — OCR (pytesseract)
# ============================================================

def extrair_ocr(filepath: str, paginas: Optional[list] = None):
    """
    Usado APENAS quando os outros extratores falham (PDF escaneado).
    Requer Tesseract instalado no SO.
    """
    if not HAS_OCR:
        return []

    resultados = []
    try:
        imgs = convert_from_path(filepath)
        for i, img in enumerate(imgs, start=1):
            if paginas and i not in paginas:
                continue
            try:
                texto = pytesseract.image_to_string(img, lang='por')
                linhas = [l.strip() for l in texto.splitlines() if l.strip()]
                if len(linhas) < 3:
                    resultados.append((i, None, 0.0,
                                       [f'OCR: pág {i} vazia']))
                    continue
                # Heurística simples: divide cada linha por 2+ espaços
                rows = [re.split(r'\s{2,}', l) for l in linhas]
                ncols = max(len(r) for r in rows)
                rows = [r + [''] * (ncols - len(r)) for r in rows]
                df = pd.DataFrame(rows[1:], columns=rows[0]).fillna('').astype(str)
                score = avaliar_qualidade(df) * 0.85  # OCR é menos confiável
                resultados.append((i, df, score,
                                   [f'OCR usado pág {i} (PDF escaneado?)']))
            except Exception as e:
                resultados.append((i, None, 0.0,
                                   [f'OCR erro pág {i}: {e}']))
    except Exception as e:
        return [(0, None, 0.0, [f'OCR falhou: {e}'])]

    return resultados


# ============================================================
# ORQUESTRADOR — pipeline em cascata
# ============================================================

def extrair_pdf(filepath: str, filename: str = '') -> tuple[pd.DataFrame, list]:
    """
    Pipeline principal. Retorna (DataFrame consolidado, lista de warnings).

    O DataFrame agrega todas as páginas com sucesso. Warnings descrevem
    cada etapa: extrator escolhido, score, páginas em quarentena.
    """
    nome = filename or Path(filepath).name
    warnings_global = []

    if not (HAS_PDFPLUMBER or HAS_PYMUPDF):
        return pd.DataFrame(), [
            f'[{nome}] Nenhum extrator de PDF instalado. '
            'Instale: pip install pdfplumber pymupdf'
        ]

    # Mapa página → melhor resultado
    melhor_por_pagina: dict[int, tuple[pd.DataFrame, float, str]] = {}
    paginas_falhas = set()

    pipeline = [
        ('pdfplumber-lattice', lambda: extrair_pdfplumber(filepath, 'lattice')),
        ('pdfplumber-stream',  lambda: extrair_pdfplumber(filepath, 'stream')),
        ('pymupdf',            lambda: extrair_pymupdf(filepath)),
    ]

    for nome_etapa, fn in pipeline:
        try:
            resultados = fn()
        except Exception as e:
            warnings_global.append(f'[{nome}] {nome_etapa} crashou: {e}')
            continue

        for pagina, df, score, ws in resultados:
            for w in ws:
                warnings_global.append(f'[{nome}] {w}')
            if df is None or score < SCORE_MINIMO:
                paginas_falhas.add(pagina)
                continue
            atual = melhor_por_pagina.get(pagina)
            if atual is None or score > atual[1]:
                melhor_por_pagina[pagina] = (df, score, nome_etapa)
                paginas_falhas.discard(pagina)

    # Tenta OCR nas páginas que ainda falharam
    if paginas_falhas and HAS_OCR:
        warnings_global.append(
            f'[{nome}] Tentando OCR em {len(paginas_falhas)} páginas...'
        )
        for pagina, df, score, ws in extrair_ocr(filepath, list(paginas_falhas)):
            for w in ws:
                warnings_global.append(f'[{nome}] {w}')
            if df is not None and score >= SCORE_MINIMO * 0.7:
                melhor_por_pagina[pagina] = (df, score, 'ocr')
                paginas_falhas.discard(pagina)

    # Quarentena: páginas que ninguém conseguiu
    for p in sorted(paginas_falhas):
        warnings_global.append(
            f'[{nome}] ⚠ Página {p} em QUARENTENA — revisão manual necessária'
        )

    if not melhor_por_pagina:
        warnings_global.append(
            f'[{nome}] ❌ Nenhuma página extraída com qualidade aceitável. '
            'Considere converter manualmente para CSV/XLSX.'
        )
        return pd.DataFrame(), warnings_global

    # Concatena páginas em ordem, alinhando colunas pela primeira
    dfs_ordenados = [melhor_por_pagina[p][0] for p in sorted(melhor_por_pagina)]
    base_cols = list(dfs_ordenados[0].columns)

    normalizados = []
    for df in dfs_ordenados:
        # Desambigua colunas duplicadas antes de concatenar
        seen = {}
        new_cols = []
        for c in df.columns:
            if c in seen:
                seen[c] += 1
                new_cols.append(f'{c}_{seen[c]}')
            else:
                seen[c] = 0
                new_cols.append(c)
        df = df.copy()
        df.columns = new_cols

        if list(df.columns) == base_cols:
            normalizados.append(df)
        elif len(df.columns) == len(base_cols):
            df2 = df.copy()
            df2.columns = base_cols
            normalizados.append(df2)
        else:
            warnings_global.append(
                f'[{nome}] Página com {len(df.columns)} cols ≠ base {len(base_cols)} '
                '— concatenada com merge tolerante'
            )
            normalizados.append(df)

    df_final = pd.concat(normalizados, ignore_index=True, sort=False).fillna('')

    # Resumo dos extratores usados
    extratores_usados = {}
    for p, (_, score, etapa) in melhor_por_pagina.items():
        extratores_usados.setdefault(etapa, []).append(p)
    for etapa, paginas in extratores_usados.items():
        warnings_global.append(
            f'[{nome}] ✓ {etapa}: {len(paginas)} pág(s) extraídas'
        )

    return df_final, warnings_global
