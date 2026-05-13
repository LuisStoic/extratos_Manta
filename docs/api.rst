Referência da API interna
=========================

Documentação gerada automaticamente a partir das docstrings.

Módulo principal — ``app``
--------------------------

.. automodule:: app
   :members:
   :exclude-members: SESSION, CFG
   :no-undoc-members:

.. note::
   ``SESSION`` é um :class:`werkzeug.local.LocalProxy` resolvido por
   cookie a cada request. Para inspeção em testes, use
   ``_DEFAULT_SESSION`` (compartilhado quando ``TESTING=True``).

Extração de PDF — ``pdf_extractor``
-----------------------------------

.. automodule:: pdf_extractor
   :members:
   :no-undoc-members:

Detectores de banco — ``detectores_banco``
------------------------------------------

.. automodule:: detectores_banco
   :members:
   :no-undoc-members:

Resolução de conta — ``resolucao_conta``
----------------------------------------

.. automodule:: resolucao_conta
   :members:
   :no-undoc-members:
