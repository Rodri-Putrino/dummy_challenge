{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}

{% macro pending(alias) -%}
  {{ alias }}.batch_seq > {{ var('lower_seq', 0) | int }}
  and {{ alias }}.batch_seq <= {{ var('upper_seq', 9223372036854775807) | int }}
{%- endmacro %}

{% macro safe_transaction_at(expression) -%}
case
  when {{ expression }} ~ '^([0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{4}/[0-9]{2}/[0-9]{2})$'
    or {{ expression }} ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,6})?(Z|[+-][0-9]{2}:[0-9]{2})?$'
  then case
    when pg_input_is_valid(replace({{ expression }}, '/', '-'), 'timestamp with time zone')
    then replace({{ expression }}, '/', '-')::timestamptz
  end
end
{%- endmacro %}

{% macro safe_amount(expression) -%}
case when {{ expression }} ~ '^-?([0-9]+|[0-9]{1,3}(,[0-9]{3})+)([.][0-9]{1,2})?$'
  and length(replace({{ expression }}, ',', '')) <= 24
then case when abs(replace({{ expression }}, ',', '')::numeric) < 10000000000000000
  then replace({{ expression }}, ',', '')::numeric(18,2)
end end
{%- endmacro %}

{% macro delete_affected_dates() %}
  {% if is_incremental() %}
    delete from {{ this }} where business_date in (
      select distinct business_date from {{ ref('stg_transactions') }} s
      where s.validation_errors is null and {{ pending('s') }}
    )
  {% else %}
    select 1
  {% endif %}
{% endmacro %}
