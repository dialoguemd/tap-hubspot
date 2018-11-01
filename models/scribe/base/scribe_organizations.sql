select id as organization_id
	, name as organization_name
	, billing_start_date is not null or id = 60 as is_paid
	, *
from scribe.organization
