with
	organizations as (
		select * from {{ ref('organizations') }}
	)

	, organizations_monthly as (
		select * from {{ ref('scribe_organizations_monthly') }}
	)

select organizations_monthly.*
	, organizations.account_id
	, organizations.account_name
from organizations_monthly
inner join organizations
	using (organization_id)
