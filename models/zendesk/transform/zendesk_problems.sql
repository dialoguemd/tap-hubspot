with tickets as (
		select * from {{ ref( 'zendesk_tickets' ) }}
	)

	, groups as (
		select * from {{ ref( 'zendesk_groups' ) }}
	)

	, tickets_tagged as (
		select * from {{ ref( 'zendesk_tickets_tagged' ) }}
	)

select tickets.ticket_id as problem_id
	, tickets.ticket_id
    , tickets.received_at
    , tickets.assignee_id
    , tickets.collaborator_ids
    , tickets.created_at
    , tickets.description
    , tickets.group_id
    , tickets.zendesk_organization_id
    , tickets.priority
    , tickets.requester_id
    , tickets.status
    , tickets.subject
    , tickets.submitter_id
    , tickets.tags
    , tickets.type
    , tickets.updated_at
    , tickets.url
    , tickets.recipient
	, coalesce(groups.group_name, 'N/A') as group_name
	, tickets_tagged.feature_tag
	, tickets_tagged.os_tag
	, tickets_tagged.platform_tag
from tickets
left join groups using (group_id)
left join tickets_tagged using (ticket_id)
where tickets.type = 'problem'
