{{ config(tags=['silver']) }}
-- Toda fila Bronze pendiente/actualizada debe aparecer exactamente una vez como
-- ganadora o en cuarentena. La reconciliación es por identidad, no sólo por conteo.
with classified as (
    select raw_row_id from {{ ref('silver_transactions') }}
    union all
    select raw_row_id from {{ ref('quarantine_transactions') }}
), scope as (
    select raw_row_id from {{ ref('stg_transactions') }} s
    {% if is_incremental() %} where {{ pending('s') }} {% endif %}
)
select s.raw_row_id
from scope s left join classified c on s.raw_row_id = c.raw_row_id
group by s.raw_row_id having count(c.raw_row_id) <> 1
