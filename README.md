# Fraud Analytics

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-56B9EB?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)


## Descrição

Projeto de análise e prevenção a fraudes desenvolvido para a empresa fictícia Horizon, utilizando Snowflake como plataforma de dados e dbt para modelagem e transformação.

- Observação: Todos os dados apresentados neste projeto são fictícios.
As bases utilizadas foram geradas por inteligência artificial exclusivamente para fins educacionais e demonstrativos.

---

### marts: 
### Views tratadas:

---

Performance Geral:

Imagine que, como analista, você precise acompanhar rapidamente o desempenho consolidado do setor.
O Performance Horizon foi desenvolvido com esse propósito: oferecer uma visão executiva e objetiva dos principais indicadores, permitindo análise rápida e tomada de decisão mais eficiente.

O relatório apresenta os indicadores-chave de performance (KPIs) de forma resumida e estruturada, facilitando o monitoramento contínuo dos resultados.

1 - Volumetria transacional agrupado por banco e período

---

    - Quantidade total
    - Quantidade de autorizacoes
    - Quantidade aprovada
    - Valor de transacoes
    - Valor de autorizacoes
    - Valor de autorizacoes aprovadas
    - Taxa de aprovacao
    - Faturamento

2 - Tabela de alertas

---

    - Quantidade total de alertas
    - Valor somado de alertas
    - Indice de falso positivo tanto para regras soft e hard
    - Hit rate tanto para regras soft e hard

3 - Tabela de cartões cancelados com perda evitada

---

A empresa Horizon possui um sistema legado responsável pela geração de relatórios de bloqueios processados via batch.
Devido a essa arquitetura, a tabela de cartões cancelados é disponibilizada com as seguintes características (features):

    - Banco,
    - Cartão,
    - Data do Bloqueio,
    - Limite total do cartão
    - Limite que sobrou do cartão no ato do cancelamento

Por conta dessa peculiaridade do relatório, faltam informações do transacional e só conseguimos cruzar cartão com cartão, 
para mitigar isso, a coluna de transaction_id das autorizações é indexada aos bloqueios através de correlaçào temporal.
Ou seja, comparamos qual foi o ultimo alerta antes de ocorrer um bloqueio, 
todos que tiverem a diferença de dias maior que zero com o tempo menor que 91 dias e o que mais se aproximar será retornado a tabela de bloqueios.
Com isso disponibilizo a tabela de bloqueios com o id da transação facilitando futuros Joins que tanto um Analytics Engineer quanto um Analista possa fazer.

    - Nova versão da tabela contém:
    - ID da transação,
    - Banco,
    - Cartão,
    - Data do Bloqueio,
    - Limite total do cartão
    - Limite que sobrou do cartão no ato do cancelamento
    - Dias passados do ultimo alerta até acontecer o bloqueio definitivo
    - Quantidade total de cartões que sofreram cancelamento definitivo ao passar por uma de nossas regras.


Índice de risco de maquinetas:

Imagine que, como analista, você precise identificar quais maquinetas com transações recentemente aprovadas representam maior exposição a risco.
O Merchant Risk foi desenvolvido com esse objetivo: fornecer uma classificação estruturada de risco para estabelecimentos, priorizando aqueles que nunca haviam transacionado anteriormente e passaram a registrar transações aprovadas que posteriormente foram contestadas como fraude.

    Essa métrica permite identificar rapidamente comportamentos mostrando maquinetas com potenciais riscos de vulnerabilidade.
    O score varia de 1 a 10, onde:
    1 → Baixo risco
    10 → Alto risco
    Quanto maior o Risk Fraud Score, maior a probabilidade de exposição a fraude.


### Tecnologias:
<img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" width="40"/> <img src="https://www.vectorlogo.zone/logos/snowflake/snowflake-icon.svg" width="36"/> <img src="https://logo.svgcdn.com/logos/dbt-icon.svg" width="36" alt="dbt logo"/>


### Objetivo:
Construir pipelines de dados para identificaçào de padrões suspeitos e geração de alertas.

## Como rodar?
```bash
git clone <repo>
python -m pipenv sync
```

### 
