Esta API implementa:

Estrutura das tabelas muro (SZ0, SZ1, SZ2, SZ3) conforme documentação

Endpoints para receber dados do LABS:

/api/cliente - Para cadastro de clientes

/api/produto - Para cadastro de produtos

/api/pedido - Para pedidos de venda

Processamento agendado que simula a integração com o Protheus

Gestão de status de processamento (P=Processado, F=Falha)

Validações básicas dos dados recebidos

Para usar a API:

Instale as dependências: pip install flask flask-sqlalchemy schedule

Execute o script: python api.py

Envie dados para os endpoints usando JSON no formato especificado no documento

Esta é uma implementação básica que pode ser expandida com:

Autenticação JWT

Logs detalhados

Integração real com o Protheus via seus métodos nativos

Melhor tratamento de erros

Sistema de filas para processamento assíncrono

Monitoramento das execuções
