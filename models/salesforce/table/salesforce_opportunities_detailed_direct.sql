with
  opportunities_detailed as (
    select *
    from {{ ref('salesforce_opportunities_detailed') }}
  )

select *
from opportunities_detailed
where owner_id not in ('0056A000000jRF2QAM', '0056A000002aemqQAA')
  and not coalesce(self_signup_no_touch, false)
