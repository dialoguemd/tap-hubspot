with tickets as (
		select * from {{ ref( 'zendesk_tickets' ) }}
	)

	, groups as (
		select * from {{ ref( 'zendesk_groups' ) }}
	)

select tickets.*
	, coalesce(groups.group_name, 'N/A') as group_name
from tickets
left join groups using (group_id)
where tickets.type in ('problem', 'incident')
