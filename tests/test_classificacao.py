"""
Testes da classificação A/B e detecção de tipo (entrada/saida).

Cobertura alvo (v9):
  - classificar: todas as combinações de issues (data/valor/descricao
    ausentes, tipo_indefinido, unidade_incerta) e a matriz de
    confiabilidade (ALTA/MÉDIA/BAIXA).
  - detectar_tipo: cascata das 4 estratégias (Débito/Crédito separados,
    coluna Tipo textual, inferência por descrição, sinal do valor).

Placeholder — implementação nos próximos prompts.
"""
