with problems as (
		select * from {{ ref('zendesk_problems') }}
	)

	, incidents as (
		select * from {{ ref('zendesk_incidents') }}
	)

select problems.problem_id
	, problems.subject
	, problems.priority
	, problems.status
	, problems.created_at
	, problems.os_tag
	, problems.platform_tag
	, problems.feature_tag
	, CONCAT(
		'https://godialogue.zendesk.com/agent/tickets/',
		problems.problem_id
		) as url
	, problems.group_name
	, count(incidents.ticket_id) as count
	, count(incidents.ticket_id)
		filter(where date_trunc('week', incidents.created_at) =
			date_trunc('week', current_date)) as count_this_week
	, max(incidents.created_at) as last_reported_at
from problems
join incidents using (problem_id)
group by 1,2,3,4,5,6,7,8,9,10
