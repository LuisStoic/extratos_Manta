"""
Testes do detector de schema (detectar_mapa) e fallbacks por banco.

Cobertura alvo (v9):
  - detectar_mapa: match exato, de-para aprendido, fuzzy com/sem acento.
  - encontrar_unidade: patterns hardcoded, fn_patterns aprendidos, fuzzy.
  - Fallbacks B1 (BRB XLS), B2 (Stone saldo-delta), B3 (Cora/GN descrição
    composta) — isolados do pipeline de processar().

Placeholder — implementação nos próximos prompts.
"""
