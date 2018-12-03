with days as (
        select * from {{ ref( 'dimension_days' ) }}
    )

    , daily_bugs as (
        select * from {{ ref( 'product_bugs_daily' ) }}
    )

    , deploys as (
        select * from {{ ref( 'github_deploys_daily' ) }}
    )

select days.date_day
    , coalesce(all_bugs, 0) as all_bugs
    , coalesce(p1_bugs, 0) as p1_bugs
    , coalesce(p2_bugs, 0) as p2_bugs
    , coalesce(p3_bugs, 0) as p3_bugs
    , coalesce(prod_dev_deploys_count, 0) as deploys_count
from days
left join daily_bugs on days.date_day = daily_bugs.date
left join deploys on days.date_day = deploys.merged_at_date
where days.date_day < date_trunc('week', current_date)
