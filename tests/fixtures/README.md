# tests/fixtures — Política de uso

Este diretório guarda arquivos de extrato reduzidos e **anonimizados**
usados pela suíte de testes. **Nunca** comite extratos originais aqui.

## Como preparar um fixture

1. Pegue um arquivo real que tenha reproduzido um bug ou um caso de
   interesse (novo banco, formato inesperado, fallback B1/B2/B3).
2. Mantenha apenas as 10-30 primeiras linhas (o suficiente para
   exercitar o caso). O header original deve ser preservado.
3. Aplique a anonimização — **obrigatória antes de commitar**:
   - CNPJ/CPF → `00.000.000/0000-00` (ou equivalente mascarado)
   - Nomes de pessoa → `FULANO DE TAL`, `BELTRANO`, etc.
   - Nomes de empresa → `EMPRESA ABC LTDA`
   - Números de conta/agência → `1234-5`, `0001`
   - Valores podem ser mantidos (não são PII) ou alterados, desde que
     o arquivo `.expected.json` reflita os totais.
4. Nomeie seguindo o padrão `{banco}_{unidade}_{formato}_{caso}.{ext}`.
   Ex: `brb_MN610_xls_legacy_header.xls`, `cora_GN_csv_desc_ausente.csv`.
5. Crie um par `.expected.json` ao lado, com os totais esperados:

   ```json
   {
     "descricao": "BRB 610 formato legacy com header merged",
     "linhas_esperadas": 12,
     "total_entradas": 1500.00,
     "total_saidas":    800.50,
     "saldo":           699.50,
     "grupo_a_esperado": 10,
     "grupo_b_esperado":  2,
     "issues_esperados": {"data_ausente": 1, "descricao_ausente": 1}
   }
   ```

## O que NÃO colocar aqui

- Extratos originais não anonimizados (PII / dado financeiro sensível).
- Arquivos > 200 KB (testes devem ser rápidos).
- Arquivos com senhas ou PDFs criptografados.

## .gitignore

Se um dia precisar de um fixture local para debugging sem comitar,
prefixe com `_local_` — o `.gitignore` do projeto ignora esse prefixo
(ou adicione a regra se ainda não existir).
