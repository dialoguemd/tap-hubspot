with
    episodes as (
        select * from {{ ref('episodes') }}
    )

    , active_fraction as (
        select date_trunc('week', created_at) as date_week
            , count(*) filter (where first_set_active is null) * 1.0 / count(*) as fraction
        from episodes
        group by 1
    )

select *
from active_fraction
where fraction > 0.18
-- This test was calibrated on 2019-02-01 with the max fraction being 0.170
-- in the week of 2019-01-21
