{{ config(
    materialized='incremental', unique_key=['transaction_id', 'transaction_at'],
    incremental_strategy='delete+insert',
    indexes=[{'columns': ['transaction_id', 'transaction_at'], 'unique': true},
             {'columns': ['business_date']}, {'columns': ['raw_row_id'], 'unique': true}]
) }}

with candidates as (
    select s.* from {{ ref('stg_transactions') }} s
    where s.validation_errors is null
    {% if is_incremental() %}
      and exists (
        select 1 from {{ ref('stg_transactions') }} changed
        where {{ pending('changed') }} and changed.validation_errors is null
          and changed.transaction_id = s.transaction_id
          and changed.transaction_at = s.transaction_at
      )
    {% endif %}
), ranked as (
    select *, row_number() over (
        partition by transaction_id, transaction_at
        order by case when status = 'COMPLETED' then 0 else 1 end,
                 ingested_at desc, batch_seq desc, source_line desc, raw_row_id desc
    ) as duplicate_rank
    from candidates
)
select raw_row_id, batch_seq, source_line, ingest_date, ingested_at,
       transaction_id, customer_id, transaction_at, business_date, amount, status, country_code
from ranked where duplicate_rank = 1
