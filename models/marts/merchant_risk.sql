/*
Classifição de risco de estabelecimentos que nunca transacionaram e tiveram 
transações aprovadas com fraudes já contestadas representando alto risco de fraude.
Quanto maior o Risk Fraude 1-10 maior o risco.

*/

with cte_authorizations as
-- Autorizacoes
(
select
    *
from {{source('sources', 'authorization')}}
where transaction_type = 'A'
)
,
cte_last_authorizations as
(
-- Transações dos ultimos 20 dias
select
    *
from cte_authorizations
where datediff(day, trn_dt, current_date()) <= 20
and reason_code = '000'
)
,
cte_all_authorizations as 
(
-- Transações anteriores aos ultimos 21 dias
select
    merchant_id,
    count(*) qty
from cte_authorizations 
where datediff(day, trn_dt, current_date()) >= 21
and reason_code = '000'
group by all
)
,
cte_unique_blocked_cards as 
(
-- Bloqueios unicos
    select 
        *
    from {{source('sources', 'blocked_cards')}}
    qualify row_number() over (partition by card_number order by block_date desc) = 1
),
cte_blocked_cards as
(
-- Filtro somente bloqueios definitivos
    select 
        *
    from cte_unique_blocked_cards
    where coalesce(block_code, '') <> 'O'
)
,
cte_transactions as 
(
-- Inner join das transacoes dos ultimos 20 dias com as transaçòes anteriores aos ultimos 21 dias
select 
    lsta.*,
    blck.block_code
from cte_last_authorizations lsta
left join cte_all_authorizations atrn on lsta.merchant_id = atrn.merchant_id
left join cte_blocked_cards blck on lsta.card_number = blck.card_number
where atrn.merchant_id is null 

)
,
cte_ranking_scores as 
(
-- Criacao das medidas de performance e ranking_score
select 
    bank,
    merchant_id,
    to_varchar(trn_dt, 'yyyy-mm-dd') trn_dt,
    count(*) qty,
    sum( case when block_code is not null then 1 else 0 end ) frd_qty,
    sum(transaction_amount) transaction_amount,
    sum( case when block_code is not null then transaction_amount else 0 end ) frd_amount,
    div0( count(*),  sum( case when block_code is not null then 1 else 0 end ) ) fpr_qty,
    div0( sum(transaction_amount),  sum( case when block_code is not null then transaction_amount else 0 end ) ) fpr_amt,
    div0( sum( case when block_code is not null then 1 else 0 end ), count(*) ) hit_rate_qty,
    div0( sum( case when block_code is not null then transaction_amount else 0 end ), sum(transaction_amount) ) hit_rate_amt,
    ((hit_rate_qty * 0.3) + (hit_rate_amt * 0.7) ) as score_base,
    log(10, round(score_base * log(10, frd_amount + 1), 4) + 1) * log(10, frd_amount + 1) ranking_scores
    from cte_transactions
    group by all
    order by ranking_scores desc
),
cte_normalization as
(
-- Valor minimo e maximo do ranking Score
select 
    *,
        min(ranking_scores) over (partition by bank) as min_score,
        max(ranking_scores) over (partition by bank) as max_score
    from cte_ranking_scores
)
-- Transformação final das colunas e criação da Risk Score
select 
    * exclude(min_score, max_score, fpr_qty, fpr_amt, hit_rate_qty, hit_rate_amt, score_base ),
    round( 1 + ( div0( ( ranking_scores - min_score ), max_score - min_score ) )  * 9, 2 )  risk_score
from cte_normalization
where risk_score > 1.0