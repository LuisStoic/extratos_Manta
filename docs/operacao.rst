Operação
========

Rodar localmente
----------------

**Windows**

Duplo-clique em ``INICIAR.bat`` ou via PowerShell:

.. code-block:: powershell

   python -m venv venv
   .\venv\Scripts\activate
   pip install -r requirements.txt
   python app.py

**Linux / macOS**

.. code-block:: bash

   chmod +x INICIAR.sh
   ./INICIAR.sh

A aplicação sobe em ``http://127.0.0.1:5000``.

Deploy em PaaS (Render, Heroku)
-------------------------------

``Procfile`` já configurado:

.. code-block:: text

   web: gunicorn --workers 1 --threads 4 --timeout 600 --bind 0.0.0.0:$PORT app:app

Decisões intencionais:

- ``workers=1`` — estado de sessão vive em memória do processo. Um segundo worker não veria os mesmos dados. Vários browsers conseguem usar simultaneamente porque cada um recebe um cookie ``app_session`` e tem ``SESSION`` próprio dentro do mesmo worker (ver :doc:`api`, função ``_resolve_session``).
- ``timeout=600`` — uploads de 200 MB + processamento de OCR pesado podem demorar.
- Filesystem efêmero (Render free) recicla disco a cada deploy. ``uploads/`` e ``fn_patterns`` aprendidos em runtime somem — aceito em uso eventual.

OCR opcional (PDFs escaneados)
------------------------------

``pdfplumber`` (em ``requirements.txt``) cobre a maioria dos PDFs.
Para escaneados:

.. code-block:: bash

   pip install -r requirements-ocr.txt

E binários do SO:

- **Tesseract OCR** — Windows: instalador oficial; Linux: ``apt install tesseract-ocr tesseract-ocr-por``.
- **Poppler** (para ``pdf2image``) — Windows: binários + PATH; Linux: ``apt install poppler-utils``.

.. warning::
   Em PaaS efêmero, ``pymupdf`` pode falhar na build por exigir compilação C.
   Se acontecer, mantenha apenas ``pdfplumber`` e aceite que PDFs
   escaneados não serão lidos.

Limites operacionais
--------------------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Item
     - Valor
   * - Upload máximo por request
     - 200 MB (``MAX_UPLOAD_MB``)
   * - Timeout do worker
     - 600 s
   * - TTL de arquivos em ``uploads/``
     - 24 h (limpo na boot)
   * - Cleanup automático
     - ``atexit`` + TTL na boot + ``/api/limpar`` manual
   * - Isolamento de sessão
     - Cookie ``app_session`` (UUID hex, HttpOnly, SameSite=Lax, 30 dias)
   * - Confiança mínima de unidade
     - 80% (abaixo → Grupo B)
   * - Fuzzy match de coluna
     - 0.82 (limiar do ``SequenceMatcher``)

Troubleshooting
---------------

**"Upload excedeu o limite"** — divida o pacote em múltiplos envios. O
limite é por *request*, não por sessão.

**"Nenhum arquivo processado"** após upload — verifique se a extensão
está em ``ALLOWED_EXT`` (CSV, XLS, XLSX, PDF, OFX). Conteúdo
duplicado (mesmo MD5 já enviado) também cai aqui com motivo
``Conteúdo idêntico...``.

**Banner "processando" não desaparece** — algum erro silencioso na
``processar()``. Abra DevTools → Console; ver também
``GET /api/progresso``.

**"Sessão não recuperou"** após reload — o cookie ``app_session`` é
HttpOnly, mas Render Free pode reciclar containers e perder o dict
``_SESSIONS_STORE``. Comportamento esperado em uso eventual.

**PDFs vindo vazios** — sem ``pymupdf``/``pytesseract``/``pdf2image``
instalados, OCR não roda. Ver seção acima.

**``pymupdf`` falhando na build** — pin antigo no ``requirements-ocr.txt``.
Tente atualizar ou aceitar perder OCR.
