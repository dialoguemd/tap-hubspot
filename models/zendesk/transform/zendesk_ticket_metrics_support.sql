with tickets as (
	    select * from {{ ref('zendesk_tickets_detailed') }}
	)

	, metrics as (
	    select * from {{ ref('zendesk_ticket_metrics') }}
	)

select tickets.ticket_id
    , tickets.created_at
    , tickets.subject
    , tickets.description
    , tickets.status
    , tickets.tags
    , tickets.recipient
    , tickets.group_name
    , tickets.assigned_user
    , metrics.first_response_time_minutes_biz
    , metrics.first_response_time_minutes
    , metrics.time_to_resolve_minutes_biz
    , metrics.time_to_resolve_minutes
from tickets
left join metrics
	using (ticket_id)
where tickets.group_name = 'Support'
