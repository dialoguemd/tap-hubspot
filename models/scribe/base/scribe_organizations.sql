select id as organization_id
	, name as organization_name
	, created
	, email_preference
	, billing_method
	-- FIXME: remove exception when the data is updated in Maestro
	-- see task in Maestro's backlog
	, date_trunc('day', coalesce(billing_start_date, created))
		as billing_start_date
	, tax_province
	, chargebee_id
	, imported_at
from scribe.organization
