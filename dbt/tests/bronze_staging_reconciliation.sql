{{ config(tags=['silver']) }}
select b.raw_row_id from {{ source('bronze', 'transactions_raw') }} b
left join {{ ref('stg_transactions') }} s on b.raw_row_id = s.raw_row_id
where {{ pending('b') }} and s.raw_row_id is null
