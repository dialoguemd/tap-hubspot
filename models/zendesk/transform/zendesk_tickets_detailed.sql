with tickets as (
	    select * from {{ ref('zendesk_tickets') }}
	)

	, groups as (
	    select * from {{ ref('zendesk_groups') }}
	)

	, users as (
	    select * from {{ ref('zendesk_users') }}
	)

select tickets.ticket_id
    , tickets.created_at
    , tickets.subject
    , tickets.description
    , tickets.status
    , tickets.tags
    , tickets.recipient
    , groups.group_name
    , users.name as assigned_user
from tickets
left join groups
	using (group_id)
left join users
	on tickets.assignee_id = users.user_id
