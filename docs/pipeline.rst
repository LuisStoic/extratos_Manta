Pipeline em 5 etapas
====================

::

   Upload → Verificação → Processamento → Revisão → Exportação

Cada etapa tem rotas próprias no Flask e mantém estado em ``SESSION``
(dict isolado por cookie ``app_session``, ver :doc:`api`).

1. Upload
---------

Rota: ``POST /api/upload``.

Aceita CSV, XLS, XLSX, PDF e OFX. Limite **200 MB por request**
(``MAX_UPLOAD_MB`` em :mod:`app`). Três camadas de validação:

1. **Extensão** — rejeita o que não está em ``ALLOWED_EXT``.
2. **Nome duplicado na sessão** — rejeita se ``secure_filename`` colidir.
3. **Conteúdo duplicado (MD5)** — rejeita após salvar, se hash já existir.

**Colisão no disco:** se o nome sobrevive aos checks acima mas
``UPLOAD_FOLDER / fn`` já existe (sessão anterior abandonada,
crash), o arquivo recebe sufixo ``_YYYYMMDD_HHMMSS`` antes da
extensão em vez de sobrescrever silenciosamente.

**Erros:**

- HTTP 413 ``upload_excedeu_limite`` — envio total acima do limite. O frontend exibe ``r.mensagem``.

2. Verificação
--------------

A unidade de negócio é identificada por três fontes ranqueadas:

1. **Cabeçalho do arquivo** (banco/agência/conta extraídos), via
   :func:`detectores_banco.detectar_cabecalho`. Fonte primária.
2. **Match em ``contas_bancarias``** via :func:`resolucao_conta.resolver_conta` — completa > CNPJ > parcial (banco+conta).
3. **Fuzzy match no nome do arquivo** (``encontrar_unidade``, limiar 80%) — só usada como reforço, nunca decide sozinha.

A função :func:`resolucao_conta.validar_cruzado` produz a matriz
cabeçalho × nome com 5 status: ``concordam``, ``apenas_cabecalho``,
``apenas_nome``, ``conflito``, ``nenhum``, mais o ``modo_legado`` quando
não há tabela-mãe cadastrada. Em ``conflito`` o operador precisa
escolher e justificar (campo ``motivo`` obrigatório, registrado em
``audit_log``).

3. Processamento
----------------

Rota: ``POST /api/processar``.

Para cada arquivo:

1. ``ler_df`` (em :mod:`app`) carrega o DataFrame respeitando o formato (CSV/Excel/OFX/PDF).
2. ``detectar_mapa`` mapeia colunas para o schema âncora — 3 camadas:

   a. De-para salvo em ``config.json['depara']`` (overrides manuais).
   b. Match exato em ``ANCHOR_MAPS`` (sinônimos por anchor, ``norm_col`` normaliza).
   c. Fuzzy SequenceMatcher (limiar 0.82).

3. ``classificar`` decide Grupo A vs Grupo B (ver :doc:`schema`).
4. Fallbacks específicos de banco (B1, B2, B3) atuam quando aplicável.

4. Revisão
----------

Rota: ``GET /api/lancamentos?grupo=A|B&page=...&per_page=...``.

Operador valida lançamentos do Grupo B. Ações:

- **Individual** — edição via modal (``POST /api/lancamento/<id>``).
- **Em lote** — confirmar/excluir subgrupo homogêneo (``POST /api/lote``):
  por **issue** (tipo de problema), **arquivo** ou **unidade**.

5. Exportação
-------------

Rota: ``GET /api/exportar``.

Gera ``BD_Extratos_<timestamp>.xlsx`` com:

- Aba **Dados** — schema âncora + colunas de rastreabilidade
  (``Conta_Id``, ``Metodo_Atribuicao``, ``Confiab_Rastreabilidade``).
- Aba **De-Para** — mapeamento de colunas detectado.
- Aba **Resumo** — sumário financeiro (entradas, saídas, saldo, contagens).
- Aba **Legenda** — descrição dos campos.

**Importante:** export NÃO limpa ``SESSION``. Re-export do mesmo dataset
após ajuste de Grupo B é UX comum.

Cleanup de uploads
------------------

Três escotilhas garantem "sem resquícios":

- ``/api/limpar`` (POST) — manual, via botão **Recomeçar**.
- :func:`app._cleanup_uploads_velhos` — varre na boot, remove arquivos
  com mtime > ``UPLOAD_TTL_HORAS`` (24 h por padrão).
- :data:`atexit.register` — limpa no shutdown gracioso (não dispara em SIGKILL — daí a TTL).

Em todas as três, ``.gitkeep`` é preservado.
