# Fraud Analytics
Projeto de análise de fraude para uma empresa ficticia chamada Horizon utilizando Snowflake e DBT.

### Descrição

## marts: 
# Views tratadas:

Performance Geral:

    Imagine que você analista precise visualizar rapidamente como andam os resultados gerais do setor, para isso você pode conferir em um relatório fechado, o performance horizon foi criado justamente para isso, visualizar indicadores chaves de forma resumida.

    1 - Volumetria transacional agrupado por banco e periodo
    ---------------------------------------------

    Quantidade total
    Quantidade de autorizacoes
    Quantidade aprovada
    Valor de transacoes
    Valor de autorizacoes
    Valor de autorizacoes aprovadas
    Taxa de aprovacao
    Faturamento

    2 - Tabela de alertas
    Quantidade total de alertas
    Valor somado de alertas
    Indice de falso positivo tanto para regras soft e hard
    Hit rate tanto para regras soft e hard

    3 - Tabela de cartões cancelados com perda evitada
    A empresa horizon dispoe de um sistema legado que gera relatórios de bloqueios via batch
    Por conta disso a tabela de cartões cancelados vem com as seguintes features:

    Banco,
    Cartão,
    Data do Bloqueio,
    Limite total do cartão
    Limite que sobrou do cartão no ato do cancelamento

    Por conta dessa peculiaridade do relatório, faltam informações do transacionao e só conseguimos cruzar cartão com cartão, 
    para mitigar isso, a coluna de transaction_id das autorizações é indexada aos bloqueios através de correlaçào temporal.
    Ou seja, comparamos qual foi o ultimo alerta antes de ocorrer um bloqueio, 
    todos que tiverem a diferença de dias maior que zero com o tempo menor que 91 dias e o que mais se aproximar será retornado a tabela de bloqueios.
    Com isso disponibilizo a tabela de bloqueios com o id da transação facilitando futuros Joins que tanto um Analytics Engineer quanto um Analista possa fazer.

    Nova versão da tabela contém:
    ID da transação,
    Banco,
    Cartão,
    Data do Bloqueio,
    Limite total do cartão
    Limite que sobrou do cartão no ato do cancelamento
    Dias passados do ultimo alerta até acontecer o bloqueio definitivo

    -----
    Quantidade total de cartões que sofreram cancelamento definitivo ao passar por uma de nossas regras.


Índice de risco de maquinetas:
    Imagine que, como analista, você precise identificar quais maquinetas com transações recentemente aprovadas representam maior exposição a risco.

    O Merchant Risk foi desenvolvido com esse objetivo: fornecer uma classificação estruturada de risco para estabelecimentos, priorizando aqueles que nunca haviam transacionado anteriormente e passaram a registrar transações aprovadas que posteriormente foram contestadas como fraude.

    Essa métrica permite identificar rapidamente comportamentos atípicos e potenciais pontos de vulnerabilidade na operação.
    O score varia de 1 a 10, onde:
    1 → Baixo risco
    10 → Alto risco
    Quanto maior o Risk Fraud Score, maior a probabilidade de exposição a fraude.


### Tecnologias:
- Python
- Snowflake
- DBT CLI


### Objetivo:
Construir pipelines de dados para identificaçào de padrões suspeitos e geração de alertas.

## Como rodar?
```bash
git clone <repo>
python -m pipenv sync
```

### 
