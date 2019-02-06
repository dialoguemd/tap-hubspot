select id as plan_id
	, organization_id
	, created as created_at
	, during
	, description_en
	, description_fr
	, name_en as plan_name
	, name_fr as plan_name_fr
	, stripe_plan_id
	, case
		-- exception for WeSpeakStudent: org that paid by consult
		when organization_id = '230' then 'fixed'
		-- exception for Dialogue
		when organization_id in ('61', '18', '251') then 'free'
		-- fix for organizations that churned before the new billing system
		when organization_id in ('47', '84') then 'dynamic'
		-- fix for End to End networks
		when organization_id = '56' then 'dynamic'
		-- New organizations are always created with a charge_strategy
		-- Some legacy free orgs did not have a charge_strategy
		else coalesce(charge_strategy, 'free')
	end as charge_strategy
	, case
		when organization_id = '230' then .1
		when organization_id in ('61', '18', '251') then 0
		when organization_id in ('47', '84') then 9
		when organization_id = '56' then 15
		when charge_strategy = 'auto_dynamic'
		then 15
		else coalesce(charge_price, 0)
	end as charge_price
	, imported_at
from scribe.plan
