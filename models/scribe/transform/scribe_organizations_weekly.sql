with
	organizations_daily as (
		select * from {{ ref('scribe_organizations_daily') }}
	)

	, organizations_weekly as (
		select date_week
			, organization_id
			, organization_name
			, is_paid
			, billing_start_date
			, charge_strategy
			, charge_price
			, charge_price_mental_health
			, charge_price_24_7
			, avg(active_contracts) as active_contracts
		from organizations_daily
		group by 1,2,3,4,5,6,7,8,9
	)

select date_week
	, organization_id
	, organization_name
	, is_paid
	, billing_start_date
	, charge_strategy
	, charge_price
	, charge_price_mental_health
	, charge_price_24_7
	, active_contracts
	, case
		when charge_strategy = 'free' then 0
		when charge_strategy = 'dynamic' then active_contracts * charge_price
		when charge_strategy = 'fixed' then charge_price
		when charge_strategy = 'auto_dynamic'
		then
			case
				when active_contracts = 0 then 0
				when active_contracts < 250 then greatest(200, 15 * active_contracts)
				else 13 * active_contracts
			end
	end as price_monthly
from organizations_weekly
