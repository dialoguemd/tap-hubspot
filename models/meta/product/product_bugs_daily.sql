with bugs as (
        select * from {{ ref( 'product_bugs' ) }}
    )

select date_trunc('day', created_at) as date
    , count(created_at) as all_bugs
    , count(created_at) filter(where issue_type = 'P3 Bug') as p3_bugs
    , count(created_at) filter(where issue_type = 'P1 Bug') as p1_bugs
    , count(created_at) filter(where issue_type = 'P2 Bug') as p2_bugs
from bugs
group by 1
