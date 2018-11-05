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
			, paid_employees
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
			, avg(paid_employees) as paid_employees
			, min(paid_employees) as min_paid_employees
			, max(paid_employees) as max_paid_employees
			, max(paid_employees) filter(where rank_asc	= 1) as paid_employees_first_day
			, max(paid_employees) filter(where rank_desc = 1) as paid_employees_last_day
		from organizations_daily_ranked
		group by 1,2,3,4,5,6,7
	)

select date_month
	, organization_id
	, organization_name
	, paid_employees
	, min_paid_employees
	, max_paid_employees
	, is_paid
	, billing_start_date
	, case
		when charge_strategy = 'free' then 0
		when charge_strategy = 'dynamic' then paid_employees * charge_price
		when charge_strategy = 'fixed' then charge_price
		when charge_strategy = 'auto_dynamic'
		then
			case
				when paid_employees = 0 then 0
				when paid_employees < 250 then greatest(200, 15 * paid_employees)
				else 13 * paid_employees
			end
	end as price_monthly
from organizations_monthly
