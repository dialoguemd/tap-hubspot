with dates as (
        select * from {{ ref( 'dimension_days' ) }}
    )

    , bugs as (
        select * from {{ ref( 'product_bugs' ) }}
    )

select dates.date_day as date
	, bugs.bug_id
	, bugs.created_at
	, bugs.closed_at
	, date_part('day', dates.date_day - bugs.created_at) + 1
		as days_opened
from dates
left join bugs
	on dates.date_day >= bugs.created_at
	and (dates.date_day <= bugs.closed_at or bugs.closed_at is null)
where bugs.issue_type = 'P2 Bug'
