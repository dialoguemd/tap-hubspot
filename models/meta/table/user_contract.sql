with
	scribe_user_contract as (
		select * from {{ ref('scribe_user_contract_detailed') }}
	)

	, organizations as (
		select * from {{ ref('organizations') }}
	)

select scribe_user_contract.*
	, organizations.account_id
	, organizations.account_name
	, organizations.account_industry
from scribe_user_contract
left join organizations
	using (organization_id)
