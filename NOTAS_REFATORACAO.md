# Notas de refatoração — v9

Registro livre de observações laterais encontradas durante a refatoração:
bugs fora do escopo do prompt atual, dívidas técnicas, ideias para
depois. Cada entrada deve ser datada.

---

## 2026-04-24 — Setup da branch v9-refactor

### Leitura inicial de app.py e templates/index.html

- **Filtro de mês/ano não implementado no backend.**
  O frontend envia `{processar_todos, mes, ano}` em `POST /api/processar`
  ([templates/index.html:1004-1009](templates/index.html#L1004-L1009)), mas
  [app.py:932](app.py#L932) ignora o body da requisição. Feature morta na
  UI. Decidir: implementar no backend ou remover do frontend.

- **`ler_df` é chamado duas vezes por arquivo em `processar()`.**
  Primeira passada em [app.py:951](app.py#L951) para montar schema global;
  segunda em [app.py:987](app.py#L987) para processar. Para PDFs, isso é
  custoso. Considerar cachear o DataFrame resultante da primeira passada.

- **`parse_data` devolve string original quando nenhum formato bate**
  ([app.py:347](app.py#L347)). O classificador checa só `if not Data`, logo
  uma data inválida tipo "32/13/2025" passa como válida e nunca gera a
  issue `data_ausente`. Deveria retornar `None` em caso de falha.

- **`jsonify` é redefinido sobre o import do Flask** ([app.py:96-107](app.py#L96-L107)).
  Funciona, mas é armadilha para manutenção: quem buscar `from flask import
  jsonify` acha o original, não o wrapper. Avaliar migrar para uma função
  com nome próprio (`safe_jsonify`) ou registrar encoder custom no app.

- **Fallbacks B1/B2/B3 estão inline no loop de `processar()`.**
  Lógica específica de banco espalhada em [app.py:1027-1058](app.py#L1027-L1058).
  Adicionar um novo banco exige mexer no meio do loop principal. Candidato
  óbvio a extrair para um módulo `bank_fallbacks/` com uma interface
  uniforme `aplicar(row, inv, extras) -> row_atualizada`.

- **`_LIXO_PDF` hardcoded dentro do loop** ([app.py:1008-1011](app.py#L1008-L1011)).
  Além de ser set recriado por iteração (custo baixo mas desnecessário), a
  lista é genérica — descarta qualquer linha com descrição `'saldo'`,
  inclusive transações legítimas.

- **`_md5` é chamado para todo upload antes da dedupe de conteúdo.**
  Em [app.py:830-836](app.py#L830-L836). Limite de 100 MB → pior caso 100 MB
  de I/O por arquivo só para checar duplicata. Barato em SSD mas
  poderíamos combinar `(size, mtime)` como filtro rápido primeiro.

- **Bug sutil em `acao='editar'`.** Em [app.py:1167](app.py#L1167), após
  edição manual, `encontrar_unidade(l['arquivo'])[1]` é chamado novamente
  para recalcular `unit_conf`. Mas se o operador mudou a **unidade** na UI,
  a confiança deveria ser 100 (foi confirmada manualmente), não a do
  fuzzy match do filename. Resultado: linha pode ir para A/B incorretamente
  com base em confiança do nome do arquivo, não da escolha manual.

- **`SESSION` é global mutável com `threaded=True`.**
  [app.py:228](app.py#L228) + [app.py:1672](app.py#L1672). Aplicação é
  single-user hoje, mas requisições concorrentes (upload+preview+gate)
  podem race. Fora de escopo para v9 mas é bomba-relógio se for deployar
  com múltiplos usuários.
