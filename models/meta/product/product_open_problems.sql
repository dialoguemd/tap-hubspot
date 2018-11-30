with dates as (
        select * from {{ ref('dimension_days') }}
    )

    , bugs as (
        select * from {{ ref('product_bugs') }}
    )

select date_day
    , count(distinct bug_id) as all_bugs
    , count(distinct bug_id) filter(where issue_type = 'P1 Bug') as p1_bugs
    , count(distinct bug_id) filter(where issue_type = 'P2 Bug') as p2_bugs
    , count(distinct bug_id) filter(where issue_type = 'P3 Bug') as p3_bugs
from dates
left join bugs
    on dates.date_day between bugs.created_at and bugs.closed_at
group by 1
