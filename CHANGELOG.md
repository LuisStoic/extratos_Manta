# Changelog

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/).
Versionamento semântico quando aplicável.

## v9.0 (em desenvolvimento)

_Refactor da base v8.0 — em andamento na branch `v9-refactor`._

### Adicionado
- **Tabela-mãe de contas bancárias** (`config.json::contas_bancarias`) ligando
  banco+agência+conta+CNPJ a uma `unit_id`. Campos: `id`, `banco_nome`,
  `banco_codigo`, `agencia`, `conta`, `cnpj_titular`, `unit_id`,
  `ativo_desde`, `ativo_ate`, `observacao`.
- Módulo `detectores_banco.py`: detecção de cabeçalho (fonte primária de
  identificação de conta). Detectores: BB, Stone completos; Cora e BRB
  parciais (só identificam o banco — TODO v9.1 para ag/conta).
- Módulo `resolucao_conta.py`:
  - `resolver_conta`: match completo > CNPJ > parcial (banco+conta).
  - `validar_cruzado`: matriz cabeçalho × nome do arquivo (5 casos +
    modo_legado). Nome do arquivo nunca decide sozinho.
- Rota `GET/POST/PUT/DELETE /api/contas` (CRUD da tabela-mãe, validando
  `unit_id` contra `unidades`).
- Rota `POST /api/confirmar-conta` para resolver conflitos e casos de
  identificação parcial. Exige `motivo` quando o operador diverge do
  cabeçalho; tudo gravado em `SESSION['conta_por_arquivo_meta']`.
- `SESSION['audit_log']` registra cada resolução manual com timestamp,
  método original, conta do cabeçalho, sugestão pelo nome e escolha humana.
- Schema do lançamento: `conta_id`, `metodo_atribuicao`,
  `Confiab_Rastreabilidade` (ALTA|MEDIA|BAIXA, distinta de `Confiabilidade`).
- Colunas Excel: `Conta_Id`, `Metodo_Atribuicao`, `Confiab_Rastreabilidade`.
  Coluna `Conta` agora é derivada via join com `contas_bancarias[conta_id]`.
- UI: card de verificação com banner colorido por status, colunas lado a
  lado (cabeçalho × nome do arquivo), botões de resolução de conflito com
  motivo obrigatório. Aba "Contas Bancárias" no modal de Configurações.
- Migração automática em `load_config()`: quando um config v8 é lido pela
  primeira vez (fn_patterns populado, contas_bancarias vazia), anexa aviso
  em NOTAS_REFATORACAO.md listando entradas para migração manual.
- Issue `data_invalida` em `classificar()`: dispara quando o campo de data
  da fonte veio preenchido mas não pôde ser parseado (ex: `'00/00/0000'`,
  `'31/02/2026'`). Mutuamente exclusivo com `data_ausente`.
- Parâmetro `preservar_raw` em `parse_data()`: retorna tupla
  `(iso_ou_None, raw)` — permite distinguir ausência de fonte (raw vazio)
  de falha de parse (raw preenchido).
- Campo `Data_Raw` nos lançamentos normalizados (`row_norm` em
  `processar()`), guardando a string original da célula de data.
- Coluna `Data_Raw` condicional em `BD_Extratos.xlsx`: aparece entre
  `Data` e `Valor` apenas quando houver lançamentos com `data_invalida`.
- Suíte de testes `tests/test_parsing.py` e `tests/test_classificacao.py`
  (57 casos — parse_valor 100% cov, parse_data 90,9% cov).

### Alterado
- **`encontrar_unidade` foi rebaixada a controle de confirmação secundária.**
  Seu resultado só decide atribuição quando o sistema está em `modo_legado`
  (contas_bancarias vazia). Fora disso, o cabeçalho decide; o nome do
  arquivo apenas confirma ou questiona.
- `processar()` agora abre o arquivo para ler cabeçalho antes do loop de
  linhas, cruza com o nome do arquivo e aplica a matriz de decisão.
  Arquivos em conflito/apenas_nome/nenhum sem confirmação humana são
  **bloqueados** (não entram em `lancamentos`). Lista retornada em
  `resp['bloqueados']`.
- Gate 2 (Verificação → Processar) agora valida que toda a seleção tem
  resolução antes de liberar o avanço. Em `modo_legado` libera com aviso.
- `parse_valor` reescrito com regra do último separador decimal:
  suporta formatos BR, US/canônico e misturados (`1.500.000,00`,
  `1,500,000.00`), além de parênteses contábeis (`(1.500,00)` → `-1500`).
- `parse_data` agora retorna `None` quando nenhum formato bate ou a
  data é calendaricamente inválida (antes preservava a string crua,
  causando passagem silenciosa de `'00/00/0000'` para Grupo A).

### Corrigido
- `'00/00/0000'` e outras datas sentinela não são mais tratadas como
  datas válidas (reproduzido em 39 linhas da base real). Agora caem
  em `data_invalida` → Grupo B, BAIXA.
- `parse_valor` de números em formato US/canônico com milhar
  (`"1,500,000.00"`): antes o `replace('.', '')` seguido de
  `replace(',', '.')` corrompia o decimal e resultado virava `None`.
  Agora retorna `1500000.0`.
- `parse_valor` agora aceita parênteses contábeis (`(1.500,00)` →
  `-1500`) — antes devolvia `None` silenciosamente.
- **Bug crítico do RRS**: arquivos identificados por similaridade textual
  do nome com limiar fuzzy ≥70% (ex.: `EXTRATO_RRS_AGRONEGOCIO.xlsx` →
  "RaiaClube ASTCU") não podem mais ser atribuídos automaticamente. Se o
  cabeçalho identifica uma conta diferente da sugestão pelo nome, o
  arquivo entra em `status='conflito'` e é **bloqueado** até o operador
  decidir explicitamente — com motivo obrigatório se divergir do
  cabeçalho. Teste de regressão em `test_integracao_contas.py`.

### Removido
-

### Notas de arquitetura

- `detectores_banco.py` e `resolucao_conta.py` ficam como **módulos flat**
  na raiz, não no pacote `app/`. Motivo: Python não permite `app.py` +
  diretório `app/` coexistindo no mesmo nível (o pacote tem precedência
  no import). Ou reescrever o entry point de `INICIAR.*` ou manter o
  layout flat — escolhemos flat para não quebrar scripts de inicialização.
  `from detectores_banco import ...` e `from resolucao_conta import ...`
  funcionam em app.py e nos testes.
