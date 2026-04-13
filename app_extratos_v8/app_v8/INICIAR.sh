#!/bin/bash
echo "============================================================"
echo "  Validacao de Extratos v8.0 — Stoic Capital"
echo "============================================================"

if ! command -v python3 &>/dev/null; then
    echo "[ERRO] Python3 nao encontrado."
    exit 1
fi

if [ ! -d "venv" ]; then
    echo "Criando ambiente virtual..."
    python3 -m venv venv
fi

source venv/bin/activate
echo "Instalando dependencias..."
pip install -r requirements.txt -q
echo ""
echo "→ http://127.0.0.1:5000"
echo ""
python app.py
