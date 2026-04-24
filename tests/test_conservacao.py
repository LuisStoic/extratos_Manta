"""
Testes de conservação: para cada arquivo-fixture, a soma Entradas - Saidas
dos lançamentos normalizados deve bater com a soma esperada do extrato
original (declarada em .expected.json ao lado do fixture).

Princípio: o pipeline pode dividir, agrupar ou reclassificar linhas, mas
nunca pode criar, perder ou trocar direção de dinheiro.

Cobertura alvo (v9):
  - Cada banco/unidade tem pelo menos um caso de conservação.
  - Tolerância de ±0,01 por arredondamento.

Placeholder — implementação nos próximos prompts.
"""
