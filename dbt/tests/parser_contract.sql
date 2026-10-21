{{ config(tags=['silver']) }}
-- Oráculos de formatos y límites; el parser nunca debe abortar por un valor inválido.
with dates(raw_value, expected) as (
    values
      ('2026/01/01', '2026-01-01T00:00:00Z'::timestamptz),
      ('2026-01-01', '2026-01-01T00:00:00Z'::timestamptz),
      ('2026-01-01T03:00:00+03:00', '2026-01-01T00:00:00Z'::timestamptz),
      ('2026-01-01T00:00:00.123456Z', '2026-01-01T00:00:00.123456Z'::timestamptz),
      ('2026-01-01T12:30:00', '2026-01-01T12:30:00Z'::timestamptz),
      ('2024-02-29', '2024-02-29T00:00:00Z'::timestamptz),
      ('2026-02-29', null), ('2026-02-30', null), ('2026/01-01', null),
      ('01/02/2026', null), ('infinity', null), ('', null), (null, null)
), amounts(raw_value, expected) as (
    values
      ('0', 0::numeric), ('-10.50', -10.50), ('1,234.56', 1234.56),
      ('9999999999999999.99', 9999999999999999.99),
      ('10000000000000000', null), ('1.234', null), ('1,23', null),
      ('1.234,56', null), ('1e5', null), ('NaN', null), ('', null), (null, null)
)
select 'date' as parser, raw_value from dates
where {{ safe_transaction_at('raw_value') }} is distinct from expected
union all
select 'amount' as parser, raw_value from amounts
where {{ safe_amount('raw_value') }} is distinct from expected
