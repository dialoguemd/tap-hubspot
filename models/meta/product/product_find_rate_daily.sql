with issues as (
        select * from {{ ref( 'jira_issues' ) }}
    )

select date_trunc('day', created_at) as date_day
	, count(*) filter (where (is_bug or is_sub_bug)) as bugs_found_count
	, count(*) filter (where (is_bug or is_sub_bug) and discovered_by = 'Tech')
		as bugs_found_by_tech_count
	, count(*) filter (where (is_bug or is_sub_bug) and discovered_by = 'Users')
		as bugs_found_by_users_count
from issues
where is_bug
group by 1
