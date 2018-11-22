select id as contract_id
	, created
	, lower(tstzrange(during)) as during_start
	, upper(tstzrange(during)) as during_end
	, tstzrange(during) as during
	, plan_id
	, plan_organization_id as organization_id
	, participant_id::text as participant_id
	, admin_area_id
	, admin_area_country_id
	, admin_area_iso_code
	, admin_area_name
from scribe.contract
