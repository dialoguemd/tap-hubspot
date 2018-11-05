select id as plan_id
	, organization_id
	, created
	, during
	, description_en
	, description_fr
	, name_en as plan_name
	, name_fr as plan_name_fr
	, stripe_plan_id
	, case
		when organization_id = '61' then 'free'
		-- New organizations are always created with a charge_strategy
		-- Some legacy free orgs did not have a charge_strategy
		else coalesce(charge_strategy, 'free')
	end as charge_strategy
	, case
		when organization_id = '61' then 0
		else coalesce(charge_price, 0)
	end as charge_price
from scribe.plan
