{{ config(tags=['gold']) }}
with expected as (
    select business_date, country_code,
        sum(case when status = 'COMPLETED' then amount else 0 end)::numeric(38,2) as total_billed_amount,
        avg(case when status = 'COMPLETED' then amount end)::numeric(38,4) as average_ticket,
        count(*) as unique_transaction_count,
        sum(case when status = 'COMPLETED' then 1 else 0 end) as completed_transaction_count,
        sum(case when status = 'CANCELLED' then 1 else 0 end) as cancelled_transaction_count,
        (100.0 * sum(case when status = 'CANCELLED' then 1 else 0 end) / count(*))::numeric(9,4) as cancellation_rate_pct
    from {{ ref('silver_transactions') }}
    group by business_date, country_code
)
select coalesce(g.business_date, e.business_date) as business_date,
       coalesce(g.country_code, e.country_code) as country_code
from {{ ref('daily_country_sales') }} g full join expected e
  on g.business_date = e.business_date and g.country_code = e.country_code
where g.unique_transaction_count is null or e.unique_transaction_count is null
   or g.total_billed_amount is distinct from e.total_billed_amount
   or g.average_ticket is distinct from e.average_ticket
   or g.unique_transaction_count is distinct from e.unique_transaction_count
   or g.completed_transaction_count is distinct from e.completed_transaction_count
   or g.cancelled_transaction_count is distinct from e.cancelled_transaction_count
   or g.cancellation_rate_pct is distinct from e.cancellation_rate_pct
   or g.cancellation_rate_pct not between 0 and 100
