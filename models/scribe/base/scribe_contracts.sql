select id as contract_id
	, created
	, tstzrange(
		date_trunc('day', lower(tstzrange(during)))
		, date_trunc('day', upper(tstzrange(during))) + interval '1 day'
	) as during
	, plan_id
	, plan_organization_id as organization_id
	, participant_id::text as participant_id
	, admin_area_id
	, admin_area_country_id
	, admin_area_iso_code
	, admin_area_name
from scribe.contract
