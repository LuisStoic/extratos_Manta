# Validação de Extratos — Stoic Capital

Aplicação Flask para **normalizar, classificar e validar extratos bancários**
de múltiplas instituições financeiras brasileiras. Recebe CSV/XLSX/XLS/OFX/PDF
de bancos diferentes, mapeia colunas para um schema âncora único e separa
lançamentos prontos para uso (Grupo A) dos que exigem revisão humana (Grupo B).

---

## Fluxo em 5 etapas

```
Upload → Verificação → Processamento → Revisão → Exportação
```

1. **Upload** — Arraste ou selecione arquivos. Limite 200 MB por envio.
2. **Verificação** — A aplicação detecta automaticamente a unidade de negócio
   de cada arquivo pelo cabeçalho (banco/agência/conta) e cruza com o nome do
   arquivo. Conflitos exigem confirmação humana com motivo registrado.
3. **Processamento** — Mapeia colunas para o schema âncora, classifica cada
   lançamento como Grupo A ou Grupo B e popula a sessão.
4. **Revisão** — Operador valida o Grupo B. Suporte a ações em lote por
   tipo de problema, por arquivo ou por unidade.
5. **Exportação** — Excel formatado com legenda, mapeamento de colunas,
   sumário financeiro e audit log.

---

## Schema âncora

Toda a normalização converge para este schema, independente do banco:

| Campo | Obrigatório | Origem |
|---|---|---|
| `Data` | sim | mapeado de `data`, `dt`, `date`, `data_lancamento`, … |
| `Valor` | sim | mapeado de `valor`, `vlr`, `amount`, ou calculado a partir de `Debito`/`Credito` separados |
| `Descricao` | sim | mapeado de `descricao`, `historico`, `memo`, `complemento`, … |
| `Tipo` | calculado | derivado do sinal de `Valor` ou de coluna textual D/C |
| `Conta`, `Banco`, `CNPJ`, `Centro_Custo` | opcional | enriquecimento via tabela-mãe `contas_bancarias` |

Lista completa de sinônimos em [`app.py`](app.py) → `ANCHOR_MAPS`.

Critérios de bloqueio (Grupo B): data inválida, valor ausente, descrição
vazia, tipo indeterminável, ou unidade com confiança < 80%.

---

## Bancos com tratamento específico

A maioria dos extratos é normalizada pelo mapa-mestre. Estes têm fallbacks
adicionais:

- **BB** — detecção por cabeçalho (banco/agência/conta extraídos da primeira linha).
- **BRB** — extrai data+histórico de célula mesclada `'Data Histórico'`.
- **Stone (Comprovante)** — `Valor` calculado a partir de `|Saldo Depois − Saldo Antes|`; tipo inferido de `extra_Movimentação`.
- **Cora / GN** — `Descricao` composta a partir de `extra_Transação` + `extra_Identificação`.

Detalhes em [`detectores_banco.py`](detectores_banco.py) e
[`resolucao_conta.py`](resolucao_conta.py).

---

## Rodar localmente

### Windows

Duplo-clique em `INICIAR.bat` ou:

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

### Linux / macOS

```bash
chmod +x INICIAR.sh
./INICIAR.sh
```

ou manual:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

A aplicação sobe em `http://127.0.0.1:5000`.

---

## Deploy (Render / Heroku)

O `Procfile` está pronto:

```
web: gunicorn --workers 1 --threads 4 --timeout 600 --bind 0.0.0.0:$PORT app:app
```

`workers=1` é proposital: o estado da sessão vive em memória do processo;
um segundo worker não veria os mesmos dados. Para múltiplos operadores
simultâneos, cada um receberá sua sessão isolada via cookie `app_session`.

**Filesystem efêmero (Render free):** `uploads/` e `fn_patterns` aprendidos
em runtime somem entre deploys — uso eventual aceita isso.

---

## Configuração — `config.json`

Estrutura:

```jsonc
{
  "unidades":          [ { "id": "MN303", "marca": "Manta", "desc_unidade": "Manta 303" }, ... ],
  "contas_bancarias":  [ { "id": "BB_MN712", "banco_nome": "BB", "agencia": "...", "conta": "...", "unit_id": "MN712" }, ... ],
  "depara":            { "Histórico": "Descricao", ... },
  "fn_patterns":       {  /* aprendizado runtime, não persiste em PaaS efêmero */  }
}
```

- **`unidades`** — universo de unidades de negócio reconhecidas pelo fuzzy match no nome do arquivo.
- **`contas_bancarias`** — tabela-mãe ligando banco+agência+conta+CNPJ a uma `unit_id`. Fonte primária de identificação a partir da v9.
- **`depara`** — overrides manuais de mapeamento de coluna. Use apenas para casos exóticos que o `ANCHOR_MAPS` não pega.
- **`fn_patterns`** — confirmações manuais aprendidas pelo sistema.

---

## OCR opcional (PDFs escaneados)

`pdfplumber` (em `requirements.txt`) cobre a maioria dos PDFs. Para PDFs
escaneados ou layouts onde ele falha, instale os extras:

```bash
pip install -r requirements-ocr.txt
```

E os binários do SO:

- **Tesseract OCR** — Windows: instalador oficial; Linux: `apt install tesseract-ocr tesseract-ocr-por`
- **Poppler** (para `pdf2image`) — Windows: baixar binários e adicionar ao PATH; Linux: `apt install poppler-utils`

⚠️ Em PaaS efêmero, `pymupdf` pode falhar na build por exigir compilação C.
Se acontecer, fique apenas com `pdfplumber` e aceite que PDFs escaneados
não serão lidos.

---

## Testes

```bash
pip install -r requirements-dev.txt
pytest
```

86 testes cobrindo parsing, classificação, detector de banco, resolução
de conta e integração end-to-end. Fixtures em `tests/fixtures/`
(todas anonimizadas — sem CNPJ/nome/conta reais).

---

## Estrutura do projeto

```
.
├── app.py                       # Flask + pipeline + rotas
├── pdf_extractor.py             # Cascata pdfplumber → fitz → OCR
├── detectores_banco.py          # Detecção por cabeçalho (BB, Stone, Cora, BRB)
├── resolucao_conta.py           # Matching contra contas_bancarias + validação cruzada
├── templates/index.html         # UI completa (SPA, vanilla JS)
├── config.json                  # Unidades, contas, de-para, fn_patterns
├── requirements.txt             # Deps mínimas
├── requirements-ocr.txt         # Extras para OCR
├── requirements-dev.txt         # pytest + cov
├── Procfile                     # gunicorn para PaaS
├── INICIAR.bat / INICIAR.sh     # Scripts de boot local
├── uploads/                     # Transient — limpo no shutdown + TTL 24h
├── tests/                       # 86 testes (pytest)
├── CHANGELOG.md                 # Histórico semântico
├── NOTAS_REFATORACAO.md         # Débitos técnicos e observações laterais
└── docs/                        # Sphinx (build com `make html`)
```

---

## Limites e garantias operacionais

| Item | Valor |
|---|---|
| Upload máximo por request | 200 MB |
| Timeout do worker (gunicorn) | 600 s |
| TTL de arquivos em `uploads/` | 24 h (limpo na boot) |
| Cleanup automático | `atexit` no shutdown + TTL na boot + `/api/limpar` manual |
| Isolamento de sessão | Cookie `app_session` (UUID), HttpOnly, SameSite=Lax, 30 dias |
| Schema âncora | `Data`, `Valor`, `Descricao` obrigatórios |
| Confiança mínima de unidade | 80% (abaixo disso vai para Grupo B) |

---

## Versionamento

Versão atual: **v9.x** — em desenvolvimento na branch `v9-refactor`.
Histórico completo em [CHANGELOG.md](CHANGELOG.md).

Bombas-relógio conhecidas e fora-de-escopo em
[NOTAS_REFATORACAO.md](NOTAS_REFATORACAO.md).
