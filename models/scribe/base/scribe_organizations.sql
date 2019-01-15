select id as organization_id
	, name as organization_name
	, created
	, email_preference
	, billing_method
	, date_trunc('day', coalesce(billing_start_date, created)) as billing_start_date
	, tax_province
	, chargebee_id
	, imported_at
from scribe.organization
