with problems as (
		select * from {{ ref( 'zendesk_problems' ) }}
	)

	, incidents as (
		select * from {{ ref( 'zendesk_incidents' ) }}
	)

select coalesce(problems.problem_id, incidents.ticket_id) as problem_id
    , coalesce(problems.subject, incidents.subject) as subject
    , coalesce(problems.priority, incidents.priority) as priority
    , coalesce(problems.status, incidents.status) as status
    , incidents.type
    , coalesce(problems.created_at, incidents.created_at) as problem_created_at
    , coalesce(problems.feature_tag, incidents.feature_tag) as feature_tag
    , coalesce(problems.os_tag, incidents.os_tag) as os_tag
    , coalesce(problems.platform_tag, incidents.platform_tag) as platform_tag
    , coalesce(problems.tags, incidents.tags) as tags
    , coalesce(problems.group_name, incidents.group_name) as group_name
    , incidents.created_at as reported_at
    , incidents.ticket_id as incident_id
from incidents
left join problems using (problem_id)
