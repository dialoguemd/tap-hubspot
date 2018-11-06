with
	scribe_organizations as (
		select * from {{ ref('scribe_organizations') }}
	)

	, salesforce_organizations as (
		select * from {{ ref('salesforce_scribe_organizations_detailed') }}
	)

select scribe_organizations.*
	, coalesce(salesforce_organizations.account_id, 'N/A') as account_id
	, coalesce(salesforce_organizations.account_name, 'N/A') as account_name
	, coalesce(salesforce_organizations.account_industry, 'N/A') as account_industry
from scribe_organizations
left join salesforce_organizations
	using (organization_id)
