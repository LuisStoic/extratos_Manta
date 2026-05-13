Schema, classificação e tabelas
================================

Schema âncora
-------------

Todo lançamento convertido é projetado para este schema, independente
do banco de origem.

**Obrigatórios** (ausência → Grupo B):

- ``Data`` — ISO ``YYYY-MM-DD``. Origem em ``data``, ``dt``, ``date``, ``data_lancamento`` etc.
- ``Valor`` — float. Origem em ``valor``, ``vlr``, ``amount``, ou calculado de ``Debito``/``Credito`` separados.
- ``Descricao`` — texto livre. Origem em ``descricao``, ``historico``, ``memo``, ``complemento`` etc.

**Calculado:**

- ``Tipo`` — ``entrada`` | ``saida`` | ``indefinido``. Derivado do sinal de ``Valor`` ou de coluna textual D/C.

**Opcionais** (ausência não penaliza):

- ``Conta``, ``Banco``, ``CNPJ``, ``Centro_Custo``.

**Rastreabilidade (v9):**

- ``Conta_Id`` — chave da tabela-mãe ``contas_bancarias``.
- ``Metodo_Atribuicao`` — ``cabecalho`` | ``nome_arquivo`` | ``humano``.
- ``Confiab_Rastreabilidade`` — ``ALTA`` | ``MEDIA`` | ``BAIXA``. Distinta de ``Confiabilidade`` (qualidade dos campos do lançamento).

Veja o dicionário completo em ``app._build_excel``.

Classificação Grupo A / B
-------------------------

A função ``classificar`` decide. **Bloqueia** para Grupo B se qualquer dos seguintes:

- ``data_ausente`` — campo de data não tinha valor na fonte.
- ``data_invalida`` — campo veio preenchido mas não pôde ser parseado (ex: ``'00/00/0000'``, ``'31/02/2026'``). Distinta de ``data_ausente``.
- ``valor_ausente`` — sem ``Valor`` mesmo após fallbacks.
- ``descricao_vazia`` — ``Descricao`` strip vazia.
- ``tipo_indefinido`` — não foi possível determinar entrada/saída.
- ``unidade_incerta`` — fuzzy match retornou confiança < 80%.
- ``conflito_cabecalho_nome_arquivo`` — cabeçalho e nome apontam para unidades diferentes.

Caso contrário, Grupo A.

ANCHOR_MAPS — sinônimos por anchor
----------------------------------

Definidos em :mod:`app` (constante ``ANCHOR_MAPS``). Resumo dos principais:

.. list-table::
   :header-rows: 1
   :widths: 18 60

   * - Anchor
     - Sinônimos reconhecidos (parcial)
   * - ``Data``
     - ``data``, ``dt``, ``date``, ``data_lancamento``, ``data_mov``, ``data_operacao``, …
   * - ``Valor``
     - ``valor``, ``vlr``, ``amount``, ``vl_lancamento``, ``valor_transacao``, …
   * - ``Debito``
     - ``debito``, ``saida``, ``debit``, ``valor_debito``, …
   * - ``Credito``
     - ``credito``, ``entrada``, ``credit``, ``valor_credito``, …
   * - ``Descricao``
     - ``descricao``, ``historico``, ``memo``, ``complemento``, ``detalhe``, ``discriminacao``, ``lancamento``, …
   * - ``Conta``
     - ``conta``, ``account``, ``nr_conta``, ``conta_corrente``, …
   * - ``Banco``
     - ``banco``, ``bank``, ``instituicao``, ``origem``, …
   * - ``CNPJ``
     - ``cnpj``, ``cpf_cnpj``, ``documento``, ``cpf``, …
   * - ``Centro_Custo``
     - ``centro_custo``, ``cost_center``, ``cc``, ``ccusto``, …

A normalização de comparação é feita por ``app.norm_col`` (minúsculo, sem
acentos, sem pontuação). Lista completa diretamente no fonte.

Tabela-mãe ``contas_bancarias``
-------------------------------

Estrutura por entrada:

.. code-block:: json

   {
     "id":            "BB_MN712",
     "banco_nome":    "BB",
     "banco_codigo":  "001",
     "agencia":       "3791-1",
     "conta":         "40102-5",
     "cnpj_titular":  "11.111.111/0001-11",
     "unit_id":       "MN712",
     "ativo_desde":   "2025-01-01",
     "ativo_ate":     null,
     "observacao":    ""
   }

``id`` é a chave estável que aparece em ``Conta_Id`` nos lançamentos
normalizados. Operações CRUD via ``/api/contas`` (GET/POST/PUT/DELETE).

Bancos com tratamento específico
--------------------------------

A maioria dos extratos passa pelo mapa-mestre. Estes têm fallbacks:

- **BB** — detecção por cabeçalho extrai banco/agência/conta. Implementação completa em :mod:`detectores_banco`.
- **BRB legado XLS** — célula mesclada ``Data Histórico`` é particionada em duas colunas (``Data`` + ``Histórico``).
- **Stone Comprovante (B2)** — ``Valor = |Saldo Depois − Saldo Antes|``, ``Tipo`` inferido de ``extra_Movimentação``.
- **Cora / GN (B3)** — ``Descricao`` composta de ``extra_Transação + extra_Identificação``.

Os fallbacks B1/B2/B3 estão inline em ``processar()`` no :mod:`app` — ver
:doc:`api`.
