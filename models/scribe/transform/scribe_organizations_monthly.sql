with
	organizations_daily as (
		select * from {{ ref('scribe_organizations_daily') }}
	)

	, organizations_daily_ranked as (
		select date_month
			, organization_id
			, organization_name
			, is_paid
			, billing_start_date
			, charge_strategy
			, charge_price
			, charge_price_mental_health
			, charge_price_24_7
			, active_contracts
			, row_number() over(
				partition by organization_id, date_month
				order by date_day
			) as rank_asc
			, row_number() over(
				partition by organization_id, date_month
				order by date_day desc
			) as rank_desc
		from organizations_daily
	)

	, organizations_monthly as (
		select date_month
			, organization_id
			, organization_name
			, is_paid
			, billing_start_date
			, charge_strategy
			, charge_price
			, charge_price_mental_health
			, charge_price_24_7
			, avg(active_contracts) as active_contracts
			, min(active_contracts) as min_active_contracts
			, max(active_contracts) as max_active_contracts
			, max(active_contracts) filter(where rank_asc = 1) as active_contracts_first_day
			, max(active_contracts) filter(where rank_desc = 1) as active_contracts_last_day
		from organizations_daily_ranked
		group by 1,2,3,4,5,6,7,8,9
	)

select date_month
	, organization_id
	, organization_name
	, active_contracts
	, min_active_contracts
	, max_active_contracts
	, is_paid
	, billing_start_date
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
	, case 
		when charge_strategy = 'dynamic'
			then active_contracts * charge_price_mental_health
		else 0 end as price_monthly_mental_health
	, case 
		when charge_strategy = 'dynamic'
			then active_contracts * charge_price_24_7
		else 0 end as price_monthly_24_7
from organizations_monthly
