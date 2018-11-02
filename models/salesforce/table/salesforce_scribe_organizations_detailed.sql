with
	organizations as (
		select * from {{ ref('salesforce_scribe_organizations') }}
	)

	, accounts as (
			select * from {{ ref('salesforce_accounts') }}
	)

select organizations.organization_name
	, organizations.organization_id
	, organizations.active_provinces
	, organizations.account_id
	, coalesce(accounts.account_name, 'N/A') as account_name
	, coalesce(accounts.industry, 'N/A') as account_industry
from organizations
left join accounts
	using (account_id)
