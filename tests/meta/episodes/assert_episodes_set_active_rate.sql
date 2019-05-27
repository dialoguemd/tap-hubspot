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
where fraction > 0.25
    and date_week > '2018-01-01'
    and date_week < date_trunc('week', current_timestamp)
-- This test was calibrated on 2019-04-17
