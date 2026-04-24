"""
Testes de parsing de valores e datas.

Cobertura alvo (v9):
  - parse_valor: formatos BR (",") / US ("."), sinal à direita (BRB),
    R$, NaN/vazio, separadores mistos.
  - parse_data: ISO, dd/mm/yyyy, dd-mm-yyyy, dd.mm.yyyy, yyyymmdd,
    Timestamp pandas, datetime, strings inválidas.

Placeholder — implementação nos próximos prompts.
"""
