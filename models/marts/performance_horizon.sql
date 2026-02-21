/*
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
Quantidade total de cartões que sofreram cancelamento definitivo ao passar por uma de nossas regras
*/
with cte_authorizations as 
(
--Autorizacoes
select *
    from {{source('sources', 'authorization')}}
),
cte_bronze_alerts as 
(
--Alertas
select *
    from {{source('sources', 'monitoring_alerts')}}
),
cte_blocked_cards as
(
-- Tabela de bloqueios completa
select 
    *
from {{source('sources', 'blocked_cards')}}
),
cte_unique_blocked_cards as 
(
-- Bloqueios unicos
select 
    *
from cte_blocked_cards
qualify row_number() over (partition by card_number order by block_date desc) = 1
),
cte_filtered_blocked_cards as
(
-- Filtro somente bloqueios definitivos
    select 
        *
    from cte_unique_blocked_cards
    where coalesce(block_code, '') <> 'O'
),
cte_bronze_alerts_etl as
(
-- Tabela de alertas completa
select 
    auth.*,
    alrt.* exclude(transaction_id),
    blck.block_code
from cte_authorizations auth
inner join cte_bronze_alerts alrt on auth.transaction_id = alrt.transaction_id
left join cte_filtered_blocked_cards blck on auth.card_number = blck.card_number
where auth.transaction_type = 'A'
)
,
cte_blocks_in_alerts as
(
-- Tabela de bloqueios que estão nos alertas se a diferença entre a data do bloqueio e a data da transação for maior que zero e menor que 90 dias com filtro da menor diferença de dias passados.
select 
    alrt.transaction_id,
    blck.*,
    datediff(day, blck.block_date, alrt.trn_dt ) diff_days
    from cte_filtered_blocked_cards blck
    left join cte_bronze_alerts_etl alrt on blck.card_number = alrt.card_number
    where datediff(second, blck.block_date, alrt.trn_dt ) >= 0
    and  datediff(second, blck.block_date, alrt.trn_dt ) < 7889185
    qualify row_number() over ( partition by blck.card_number order by datediff(second, blck.block_date, alrt.trn_dt ) ) = 1
),
cte_canceled_cards_enriched as
(
-- -- ETL final: Junção da tabela de bloqueios completa trazendo o ID transação dos casos elegiveis de perda evitada
select  
    blkf.transaction_id,
    blck.*,
    blkf.diff_days
    from cte_blocked_cards blck
    left join cte_blocks_in_alerts blkf on blck.card_number = blkf.card_number
    qualify row_number() over (partition by blck.card_number order by blck.block_date desc) = 1
)
,
cte_silver_alerts as
(
-- Junção da tabela de cartões cancelados que está enriquecida com o transaction_id das transações
select
    alrt.*,
    case when blck.transaction_id is not null then true else false end loss_avoided,
    coalesce(blck.card_limit_available, 0) card_limit_available,
    coalesce(blck.card_limit_total, 0) card_limit_total
from cte_bronze_alerts_etl alrt
left join cte_canceled_cards_enriched blck on alrt.transaction_id = blck.transaction_id
)
,
cte_auth_indicators as 
(
-- Principais indicadores das transações
    select
        bank,
        to_varchar(trn_dt, 'yyyy-mm') trn_dt,
        count(*) trn_quantity,
        sum(case when transaction_type = 'A' then 1 else 0 end) trn_qty_auth,
        sum(case when transaction_type = 'A' and reason_code = '000' then 1 else 0 end) auth_qty_approved,
        sum(transaction_amount) as transaction_amount,
        round(sum(case when transaction_type = 'A' then transaction_amount else 0 end), 2) as trn_amt_auth,
        round(sum(case when transaction_type = 'A' and reason_code = '000' then transaction_amount else 0 end), 2) as auth_amt_approved,
        
        round(sum(case when transaction_type = 'A' and reason_code <> '000' then 1 else 0 end) / sum(case when transaction_type = 'A' and reason_code = '000' then 1 else 0 end), 2) approved_rate,
        round( sum(case when transaction_type = 'A' and reason_code = '000' then transaction_amount else 0 end ) -
        sum(case when transaction_type = 'O' and reason_code = '000' then transaction_amount else 0 end ), 2) net_approved_credit_amout
        
    from cte_authorizations
    group by all   
),
cte_alerts_indicators as
(
-- Principais indicadores dos alertas
    select
        bank,
        to_varchar(trn_dt, 'yyyy-mm') trn_dt,
        count(*) alert_quantity,
        sum(transaction_amount) alert_transaction_amount,
        round(div0( count(*), sum( case when block_code in ('F', 'P', 'R') then 1 else 0 end ) ), 3) fpi,
        round(div0( count(*), sum( case when block_code in ('F', 'P', 'R') and decision_indicator = 'soft' then 1 else 0 end ) ), 3) fpi_soft,
        round(div0( count(*), sum( case when block_code in ('F', 'P', 'R') and decision_indicator = 'hard' then 1 else 0 end ) ), 3) fpi_hard,
        round(div0( sum( case when block_code in ('F', 'P', 'R') then 1 else 0 end ), count(*) ), 3) fpr,
        round(div0( sum( case when block_code in ('F', 'P', 'R') and decision_indicator = 'soft' then 1 else 0 end ), count(*) ), 3) fpr_soft,
        round(div0( sum( case when block_code in ('F', 'P', 'R') and decision_indicator = 'hard' then 1 else 0 end ), count(*) ), 3) fpr_hard,
        sum( case when loss_avoided = true then 1 else 0 end ) loss_avoided_qty,
        round(sum( case when loss_avoided = true then card_limit_available else 0 end ), 2) loss_avoided_amt
    from cte_silver_alerts alrt
    group by all
)
-- Juncao tabelas criação de uma OBT com todos os indicadores relevantes
    select 
        auth.*,
        alrt.* exclude(bank, trn_dt)
    from cte_auth_indicators auth
    inner join cte_alerts_indicators alrt on auth.bank = alrt.bank and auth.trn_dt = alrt.trn_dt
    order by bank, trn_dt asc