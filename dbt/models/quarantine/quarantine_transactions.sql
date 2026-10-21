{{ config(
    materialized='incremental', unique_key='raw_row_id', incremental_strategy='delete+insert',
    indexes=[{'columns': ['raw_row_id'], 'unique': true}, {'columns': ['ingest_date']},
             {'columns': ['batch_seq']}]
) }}

with candidates as (
    select s.* from {{ ref('stg_transactions') }} s
    {% if is_incremental() %}
    where {{ pending('s') }} or exists (
        select 1 from {{ ref('stg_transactions') }} changed
        where {{ pending('changed') }} and changed.validation_errors is null
          and changed.transaction_id = s.transaction_id
          and changed.transaction_at = s.transaction_at
    )
    {% endif %}
)
select c.*,
       coalesce(c.validation_errors, 'DUPLICATE_SUPERSEDED') as reason_codes,
       winner.raw_row_id as winning_raw_row_id
from candidates c
left join {{ ref('silver_transactions') }} winner
    on c.transaction_id = winner.transaction_id and c.transaction_at = winner.transaction_at
where c.validation_errors is not null or c.raw_row_id <> winner.raw_row_id
