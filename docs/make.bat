@ECHO OFF
REM make.bat para Sphinx no Windows.
REM Uso: cd docs ^& make html

pushd %~dp0

REM Sphinx via venv ativo OU instalado globalmente
set SPHINXBUILD=sphinx-build
set SOURCEDIR=.
set BUILDDIR=_build

if "%1" == "" goto help

%SPHINXBUILD% -M %1 "%SOURCEDIR%" "%BUILDDIR%" %SPHINXOPTS% %O%
goto end

:help
%SPHINXBUILD% -M help "%SOURCEDIR%" "%BUILDDIR%" %SPHINXOPTS% %O%

:end
popd
