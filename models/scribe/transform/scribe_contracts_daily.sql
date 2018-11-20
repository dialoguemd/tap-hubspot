with
  contracts as (
    select * from {{ ref('scribe_contracts_detailed') }}
  )

  , dates as (
    select generate_series('2016-10-01', current_date, interval '1 day') date_day
  )

select dates.date_day
  , count(distinct contracts.contract_id) as contract_count
  , count(distinct contracts.contract_id) filter(where contracts.is_paid)
    as contract_paid_count
from dates
inner join contracts
  on dates.date_day <@ contracts.during
group by 1
