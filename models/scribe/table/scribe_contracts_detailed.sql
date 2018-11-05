with
	contracts as (
		select * from {{ ref('scribe_contracts') }}
	)

	, organizations as (
		select * from {{ ref('scribe_organizations_detailed') }}
	)

select contracts.*
	, organizations.charge_price
	, organizations.charge_strategy
	, organizations.organization_name
	, organizations.billing_start_date
	, organizations.is_paid
from contracts
inner join organizations
	using (organization_id)
