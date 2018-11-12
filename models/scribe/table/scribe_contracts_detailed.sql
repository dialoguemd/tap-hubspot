with
	contracts as (
		select * from {{ ref('scribe_contracts') }}
	)

	, organizations as (
		select * from {{ ref('scribe_organizations_detailed') }}
	)

select contracts.contract_id
	, contracts.created
	, contracts.during
	, contracts.plan_id
	, contracts.organization_id
	, contracts.participant_id
	, contracts.admin_area_id
	, contracts.admin_area_country_id
	, contracts.admin_area_iso_code
	, coalesce(contracts.admin_area_name,
		case organizations.province
			when 'QC' then 'Quebec'
			when 'ON' then 'Ontario'
			when 'MB' then 'Manitoba'
			when 'YT' then 'Yukon'
			when 'AB' then 'Alberta'
			when 'SK' then 'Saskatchewan'
			when 'BC' then 'British Columbia'
			when 'NB' then 'New Brunswick'
			when 'NS' then 'Nova Scotia'
			else organizations.province
		end
	) as admin_area_name
	, organizations.charge_price
	, organizations.charge_strategy
	, organizations.organization_name
	, organizations.billing_start_date
	, organizations.is_paid
from contracts
inner join organizations
	using (organization_id)
