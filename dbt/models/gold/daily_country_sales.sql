-- depends_on: {{ ref('stg_transactions') }}
{{ config(
    materialized='incremental', incremental_strategy='append',
    pre_hook="{{ delete_affected_dates() }}",
    indexes=[{'columns': ['business_date', 'country_code'], 'unique': true}]
) }}

-- El pre-hook reemplaza días completos dentro de la misma transacción que el INSERT.
-- Esto también elimina grupos obsoletos si el ganador cambia de país.
with transactions as (
    select s.* from {{ ref('silver_transactions') }} s
    {% if is_incremental() %}
    where s.business_date in (
        select distinct business_date from {{ ref('stg_transactions') }} changed
        where changed.validation_errors is null and {{ pending('changed') }}
    )
    {% endif %}
)
select business_date, country_code,
    cast(sum(case when status = 'COMPLETED' then amount else 0 end) as numeric(38,2)) as total_billed_amount,
    cast(avg(case when status = 'COMPLETED' then amount end) as numeric(38,4)) as average_ticket,
    count(*) as unique_transaction_count,
    sum(case when status = 'COMPLETED' then 1 else 0 end) as completed_transaction_count,
    sum(case when status = 'CANCELLED' then 1 else 0 end) as cancelled_transaction_count,
    cast(100.0 * sum(case when status = 'CANCELLED' then 1 else 0 end) / count(*) as numeric(9,4)) as cancellation_rate_pct
from transactions
group by business_date, country_code
