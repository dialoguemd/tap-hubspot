with
	contracts as (
		select * from scribe.contract
	)

	, test_users as (
		select * from {{ ref('scribe_test_users') }}
	)

select id as contract_id
	, created
	, lower(tstzrange(during)) as during_start
	, upper(tstzrange(during)) as during_end
	, tstzrange(during) as during
	, tsrange(
		timezone('America/Montreal', lower(tstzrange(during)))
		, timezone('America/Montreal', upper(tstzrange(during)))
	) as during_est
	, plan_id
	, plan_organization_id as organization_id
	, participant_id::text as participant_id
	, admin_area_id
	, admin_area_country_id
	, admin_area_iso_code
	, admin_area_name
	, imported_at
from scribe.contract
left join test_users
	on contract.participant_id::text = test_users.user_id
where test_users.user_id is null
