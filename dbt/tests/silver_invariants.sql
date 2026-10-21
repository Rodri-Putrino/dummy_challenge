{{ config(tags=['silver']) }}
select transaction_id, transaction_at
from {{ ref('silver_transactions') }}
group by transaction_id, transaction_at
having count(*) <> 1 or min(amount) < 0
