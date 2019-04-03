with tickets as (
		select * from {{ ref('zendesk_tickets') }}
	)

	, groups as (
		select * from {{ ref('zendesk_groups') }}
	)

	, tickets_tagged as (
		select * from {{ ref('zendesk_tickets_tagged') }}
	)

select tickets.*
	, coalesce(groups.group_name, 'N/A') as group_name
	, tickets_tagged.feature_tag
	, tickets_tagged.os_tag
	, tickets_tagged.platform_tag
from tickets
left join groups using (group_id)
left join tickets_tagged using (ticket_id)
where tickets.type in ('problem', 'incident')
