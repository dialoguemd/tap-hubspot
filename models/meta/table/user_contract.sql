with
	scribe_user_contract as (
		select * from {{ ref('scribe_user_contract_detailed') }}
	)

	, organizations as (
		select * from {{ ref('organizations') }}
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

	, activated_at as (
		select patient_id as user_id
			, min(first_message_patient) as first_message_patient
			, min(first_set_active) as activated_at
		from episodes
		group by 1
	)

select scribe_user_contract.*
	-- Legacy fields
	, activated_at.first_message_patient
	, activated_at.activated_at is not null as has_first_message
	, date_trunc('month', activated_at.first_message_patient) as first_message_month

	, activated_at.activated_at
	, activated_at.activated_at is not null as is_activated
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
