with
	scribe_user_contract as (
		select * from {{ ref('scribe_user_contract_detailed') }}
	)

	, organizations as (
		select * from {{ ref('organizations') }}
	)

	, activated_at as (
		select * from {{ ref('user_activated') }}
	)

select scribe_user_contract.*
	, activated_at.activated_at
	, date_trunc('month', activated_at.activated_at) as activated_month
	, organizations.account_id
	, organizations.account_name
	, organizations.account_industry
	, organizations.features
	, organizations.has_mental_health
	, organizations.has_24_7
from scribe_user_contract
left join organizations
	using (organization_id)
left join activated_at
	on scribe_user_contract.user_id = activated_at.user_id
	and scribe_user_contract.during_end >= activated_at.activated_at
