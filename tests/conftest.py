"""
Fixtures comuns do pytest para a suíte de testes v9.

Todos os arquivos de fixture devem ser anonimizados (sem CNPJs, nomes,
contas reais). Ver tests/fixtures/README.md para a política.
"""
from pathlib import Path
import sys

import pytest


# Garante que app.py (raiz do projeto) seja importável pelos testes.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True, scope='session')
def _modo_teste_global():
    """Ativa TESTING=True para que o LocalProxy SESSION sempre resolva para
    `_DEFAULT_SESSION` (compartilhada), preservando o contrato pré-cookie
    onde testes importam `app.SESSION` diretamente e o mutam."""
    from app import app as flask_app
    flask_app.config['TESTING'] = True
    yield


@pytest.fixture
def fixtures_dir() -> Path:
    """Caminho absoluto para tests/fixtures/."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def app_client():
    """Cliente Flask em modo de teste. Sessão reiniciada a cada teste."""
    from app import app as flask_app
    from app import SESSION

    flask_app.config["TESTING"] = True
    SESSION.update({
        "arquivos": [],
        "lancamentos": [],
        "schema_map": {},
        "processado": False,
        "doc_verificados": {},
        "previews": {},
        "progresso": {"pct": 0, "msg": "", "ativo": False},
    })
    with flask_app.test_client() as client:
        yield client


@pytest.fixture
def sample_row_bb():
    """Linha-exemplo no formato BB (Débito/Crédito separados)."""
    return {
        "Data": "15/03/2025",
        "Histórico": "TED RECEBIDO",
        "Débito": "",
        "Crédito": "1000,00",
    }


@pytest.fixture
def sample_row_brb():
    """Linha-exemplo BRB (valor único, sinal à direita)."""
    return {
        "Data Lançamento": "15/03/2025",
        "Histórico": "DEBITO CONTA",
        "Valor": "500,00-",
    }
