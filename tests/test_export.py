"""
Testes da montagem do Excel (_build_excel).

Cobertura alvo (v9):
  - 4 abas presentes: Legenda, BD_Extratos, De_Para, Sumario.
  - Cabeçalho duplo em BD_Extratos (banner + nomes).
  - Colunas extra_* expandidas corretamente, sem 'Unnamed' e
    sem 'Nosso Número'.
  - Sumário bate com a lista de entrada (entradas, saidas, saldo).
  - Status 'excluido' nunca aparece na aba BD_Extratos.

Placeholder — implementação nos próximos prompts.
"""
