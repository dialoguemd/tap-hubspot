with
	organizations as (
		select * from {{ ref('organizations') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

	, scribe_plans as (
		select * from {{ ref('scribe_plans') }}
	)

	, organization_month as (
		select generate_series(
				date_trunc('month', billing_start_date)
				, date_trunc('month', current_date) + interval '1 month' - interval '1 day'
				, '1 day'
			) as date_month
			, organization_id
			, organization_name
			, account_id
			, account_name
		from organizations
		where is_paid
	)

	, monthly_contracts as (
		select organization_month.date_month
		  , organization_month.organization_id
		  , organization_month.organization_name
		  , organization_month.account_id
		  , organization_month.account_name
		  , count(distinct user_contract.user_id) as paid_employees
		from organization_month
		left join user_contract
		  on organization_month.date_month <@ user_contract.during
			and organization_month.organization_id = user_contract.organization_id
			and date_trunc('day', user_contract.billing_start_date)
			  <= organization_month.date_month
			and user_contract.is_employee
		group by 1,2,3,4,5
	)

	, monthly_employees as (
		select monthly_contracts.date_month
			, monthly_contracts.organization_id
			, monthly_contracts.organization_name
			, monthly_contracts.account_id
			, monthly_contracts.account_name
			, scribe_plans.charge_strategy
			, scribe_plans.charge_price
			, avg(monthly_contracts.paid_employees) as paid_employees
			, min(monthly_contracts.paid_employees) as min_paid_employees
		from monthly_contracts
		inner join scribe_plans
			using (organization_id)
		group by 1,2,3,4,5,6,7
	)

	, monthly_price_org as (
		select date_month
			, organization_id
			, organization_name
			, account_id
			, account_name
			, paid_employees
			, min_paid_employees
			, case
			when charge_strategy = 'dynamic'
			then paid_employees * charge_price
			when charge_strategy = 'fixed'
			then charge_price
			when charge_strategy = 'auto_dynamic'
			then
				case
					when paid_employees = 0
					then 0
					when paid_employees < 250
					then greatest(200, 15 * paid_employees)
					else 13 * paid_employees
				end
			else null
			end as monthly_price
		from monthly_employees
	)

	, monthly_price_account as (
		select date_month
			, account_id
			, account_name
			, sum(paid_employees) as paid_employees
			-- if all orgs of an account had 0 employees
			-- at some point in the month then the account in churned
			, max(min_paid_employees) as min_paid_employees
			, sum(monthly_price) as monthly_price
		from monthly_price_org
		group by 1,2,3
	)

	, monthly_price_lag as (
		select date_month
			, account_id
			, account_name
			, paid_employees
			, monthly_price
			, min_paid_employees
			, lag(min_paid_employees)
				over(
					partition by account_id
					order by date_month
				)
				as last_month_min_paid_employees
			, lag(paid_employees)
				over(
					partition by account_id
					order by date_month
				)
				as last_month_employees
			, lag(monthly_price)
				over(
					partition by account_id
					order by date_month
				)
				as last_month_price
		from monthly_price_account
	)

select date_month
	, account_id
	, account_name
	, paid_employees
	, monthly_price
	, coalesce(monthly_price, 0) - coalesce(last_month_price, 0) as price_difference
	, case
		when last_month_price is null or last_month_min_paid_employees = 0
		then 'New'
		when min_paid_employees = 0
		then 'Churned'
		when monthly_price = 0 and last_month_price > 0
		then 'Churned'
		when monthly_price - last_month_price > 0
		then 'Expansion'
		when monthly_price - last_month_price < 0
		then 'Contraction'
		when monthly_price = last_month_price
		then 'No change'
		else 'NA'
	end as churn_type
from monthly_price_lag
where (
		last_month_price <> 0
		or last_month_price is null
	)
	and (
		last_month_employees <> 0
		or last_month_employees is null
	)
