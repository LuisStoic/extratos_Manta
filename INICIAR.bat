@echo off
echo ============================================================
echo   Validacao de Extratos v8.0 — Stoic Capital
echo ============================================================
echo.

where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERRO] Python nao encontrado. Instale em python.org
    pause & exit /b
)

if not exist "venv\" (
    echo Criando ambiente virtual...
    python -m venv venv
)

call venv\Scripts\activate.bat

echo Instalando dependencias...
pip install -r requirements.txt -q

echo.
echo Iniciando servidor em http://127.0.0.1:5000
echo Pressione Ctrl+C para encerrar.
echo.
python app.py

pause
