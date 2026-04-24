# Changelog

Formato baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/).
Versionamento semântico quando aplicável.

## v9.0 (em desenvolvimento)

_Refactor da base v8.0 — em andamento na branch `v9-refactor`._

### Adicionado
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

### Removido
-
