# Documentação Sphinx

Build local da documentação técnica (autodoc das docstrings + páginas
manuais em ReST).

## Instalar

A partir da raiz do projeto, com o venv ativo:

```bash
pip install -r requirements-dev.txt
```

## Build

```bash
cd docs
make html       # Linux / macOS
make.bat html   # Windows
```

O HTML é gerado em `_build/html/`. Abra `_build/html/index.html` no
navegador.

## Estrutura

- `conf.py` — configuração do Sphinx (tema Furo, napoleon, intersphinx).
- `index.rst` — página inicial + toctree.
- `pipeline.rst` — fluxo das 5 etapas em detalhe.
- `schema.rst` — schema âncora, classificação, ANCHOR_MAPS, tabela-mãe.
- `operacao.rst` — rodar local, deploy, limites, troubleshooting.
- `api.rst` — autodoc dos módulos.
- `changelog.md` / `notas.md` — incluem os arquivos da raiz.

## Limpar build

```bash
make clean
```

## Tema

Furo (https://pradyunsg.me/furo/). Para trocar, editar `html_theme` em
`conf.py` e adicionar o pacote em `requirements-dev.txt`.
