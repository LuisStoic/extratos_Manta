"""Sphinx configuration — Validação de Extratos."""
from __future__ import annotations
import os
import sys
from datetime import datetime
from pathlib import Path

# Permite que autodoc importe os módulos da raiz do projeto.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# -- Project information -----------------------------------------------------
project   = 'Validação de Extratos'
author    = 'Stoic Capital'
copyright = f'{datetime.now():%Y}, {author}'
release   = '9.x'

# -- General configuration ---------------------------------------------------
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',          # docstrings Google/Numpy style
    'sphinx.ext.viewcode',          # link [source] em cada item
    'sphinx.ext.intersphinx',       # cross-refs para docs externas
    'sphinx_autodoc_typehints',     # render type hints como argumentos
    'myst_parser',                  # permite incluir .md (README) no toctree
]

templates_path   = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']
language         = 'pt_BR'

# Comum em docstrings curtas
napoleon_google_docstring = True
napoleon_numpy_docstring  = True
napoleon_include_init_with_doc = False

# autodoc — ordem dos membros + signatures inline
autodoc_default_options = {
    'members':           True,
    'undoc-members':     False,
    'show-inheritance':  True,
    'member-order':      'bysource',
}
autodoc_typehints = 'signature'

# Cross-refs externas
intersphinx_mapping = {
    'python':  ('https://docs.python.org/3',           None),
    'flask':   ('https://flask.palletsprojects.com/en/stable/', None),
    'pandas':  ('https://pandas.pydata.org/docs/',     None),
}

# -- HTML output -------------------------------------------------------------
html_theme        = 'furo'
html_title        = 'Validação de Extratos · v9.x'
html_static_path  = ['_static']

# MyST — permite mergulhar README.md no toctree
myst_enable_extensions = [
    'colon_fence',
    'deflist',
]

# Suppress noisy warnings do autodoc para módulos com fallback opcional
suppress_warnings = ['autodoc.import_object']
