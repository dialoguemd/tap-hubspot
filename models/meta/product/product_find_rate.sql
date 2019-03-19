with issues as (
        select * from {{ ref( 'jira_issues' ) }}
    )

select date_trunc('week', created_at) as date_week
	, count(*) filter (where is_bug) as bugs_found_count
	, count(*) filter (where is_bug and discovered_by = 'Tech')
		as bugs_found_by_tech_count
	, count(*) filter (where is_bug and discovered_by = 'Users')
		as bugs_found_by_users_count
	, count(*) filter (where is_bug and discovered_by = 'Tech') * 1.0
		/ count(*) filter (where is_bug)
		as find_rate
from issues
where is_bug
group by 1
