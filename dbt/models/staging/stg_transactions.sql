{{ config(
    materialized='incremental', unique_key='raw_row_id', incremental_strategy='delete+insert',
    indexes=[{'columns': ['raw_row_id'], 'unique': true}, {'columns': ['batch_seq']},
             {'columns': ['transaction_id', 'transaction_at']}]
) }}

with raw as (
    select * from {{ source('bronze', 'transactions_raw') }} r
    {% if is_incremental() %} where {{ pending('r') }} {% endif %}
), parsed as (
    select
        raw_row_id, batch_seq, source_line, ingest_date, ingested_at, raw_payload,
        ingestion_error,
        nullif(trim(transaction_id), '') as transaction_id,
        nullif(trim(customer_id), '') as customer_id,
        {{ safe_transaction_at('trim(transaction_date)') }} as transaction_at,
        {{ safe_amount("regexp_replace(trim(amount), '^[$€£][[:space:]]*', '')") }} as amount,
        upper(trim(status)) as status,
        upper(trim(country_code)) as country_code
    from raw
), validated as (
    select p.*,
        (p.transaction_at at time zone 'UTC')::date as business_date,
        nullif(concat_ws('|',
            p.ingestion_error,
            case when p.transaction_id is null then 'MISSING_TRANSACTION_ID' end,
            case when p.customer_id is null then 'MISSING_CUSTOMER_ID' end,
            case when p.transaction_at is null then 'INVALID_TRANSACTION_DATE' end,
            case when p.amount is null then 'NULL_OR_INVALID_AMOUNT' end,
            case when p.amount < 0 then 'NEGATIVE_AMOUNT' end,
            case when p.status = 'TEST' then 'TEST_TRANSACTION' end,
            case when p.status is null or p.status not in ('COMPLETED', 'PENDING', 'CANCELLED', 'TEST')
                then 'INVALID_STATUS' end,
            case when c.country_code is null then 'INVALID_COUNTRY_CODE' end
        ), '') as validation_errors
    from parsed p
    left join {{ ref('country_codes') }} c on p.country_code = c.country_code
)
select * from validated
