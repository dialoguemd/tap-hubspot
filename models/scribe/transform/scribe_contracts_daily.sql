with
  contracts as (
    select * from {{ ref('scribe_contracts_detailed') }}
  )

  , dates as (
    select generate_series('2016-10-01', current_date, interval '1 day') date_day
  )

  , date_ranges as (
    select date_day
      , tstzrange(date_day, date_day + interval '1 day') as date_range
    from dates
  )

select date_ranges.date_day
  , count(distinct contracts.contract_id) as contract_count
  , count(distinct contracts.contract_id) filter(where contracts.is_paid)
    as contract_paid_count
from date_ranges
inner join contracts
  on date_ranges.date_range && contracts.during
{% if target.name == 'dev' %}
  where date_day > current_date - interval '1 months'
{% endif %}
group by 1
