with
	scribe_organizations as (
		select * from {{ ref('scribe_organizations_detailed') }}
	)

	, salesforce_organizations as (
		select * from {{ ref('salesforce_scribe_organizations_detailed') }}
	)

	, salesforce_accounts as (
		select * from {{ ref('salesforce_accounts_detailed')}}
	)

select scribe_organizations.*
	, scribe_organizations.organization_id || ' - ' ||
		scribe_organizations.organization_name as organization_name_id
	, coalesce(salesforce_accounts.am_id, 'N/A') as account_manager_id
	, coalesce(salesforce_accounts.am_name, 'N/A') as account_manager_name
	, coalesce(salesforce_accounts.am_email, 'N/A') as account_manager_email
	, coalesce(salesforce_organizations.account_id, 'N/A') as account_id
	, coalesce(salesforce_organizations.account_name, 'N/A') as account_name
	, coalesce(salesforce_organizations.account_industry, 'N/A') as account_industry
from scribe_organizations
left join salesforce_organizations
	using (organization_id)
left join salesforce_accounts
	using (account_id)
