/* Cartões cancelados
---------------------------------------------

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

*/
with cte_blocked_cards as
(
-- Tabela de bloqueios completa
select 
    *
from {{source('sources', 'blocked_cards')}}
),
cte_blocked_cards_filter as
(
-- Tabela somente com bloqueios definitivos
select 
    *
from cte_blocked_cards
where coalesce(block_code, '') <> 'O'
),
cte_alerts as
(
-- Tabela de alertas completa
select 
    auth.*,
    alrt.* exclude(transaction_id)
from {{source('sources', 'authorization')}} auth
inner join {{source('sources', 'monitoring_alerts')}} alrt on auth.transaction_id = alrt.transaction_id
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
    from cte_blocked_cards_filter blck
    left join cte_alerts alrt on blck.card_number = alrt.card_number
    where datediff(second, blck.block_date, alrt.trn_dt ) >= 0
    and  datediff(second, blck.block_date, alrt.trn_dt ) < 7889185
    qualify row_number() over ( partition by blck.card_number order by datediff(second, blck.block_date, alrt.trn_dt ) ) = 1
)
-- ETL final: Junção da tabela de bloqueios completa trazendo o ID transação dos casos elegiveis de perda evitada
select  
    blkf.transaction_id,
    blck.*,
    blkf.diff_days
    from cte_blocked_cards blck
    left join cte_blocks_in_alerts blkf on blck.card_number = blkf.card_number
    qualify row_number() over (partition by blck.card_number order by blck.block_date desc) = 1
