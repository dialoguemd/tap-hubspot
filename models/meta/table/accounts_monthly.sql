with
	organizations_monthly as (
		select * from {{ ref('organizations_monthly') }}
	)

	, accounts_monthly as (
		select date_month
			, account_id
			, account_name
			, bool_or(is_paid) as is_paid
			, min(billing_start_date) as billing_start_date
			, sum(active_contracts) as active_contracts
			-- if all orgs of an account had 0 employees
			-- at some point in the month then the account in churned
			, max(min_active_contracts) as min_active_contracts
			, sum(price_monthly) as price_monthly
		from organizations_monthly
		group by 1,2,3
	)

	, accounts_monthly_lag as (
		select date_month
			, account_id
			, account_name
			, is_paid
			, date_trunc('month', billing_start_date) as billing_start_month
			, billing_start_date
			, active_contracts
			, price_monthly
			, min_active_contracts
			, coalesce(
					lag(min_active_contracts) over(
						partition by account_id
						order by date_month
					)
				, 0) as min_active_contracts_last_month
			, coalesce(
					lag(active_contracts) over(
						partition by account_id
						order by date_month
					)
				, 0) as active_contracts_last_month
			, coalesce(
					lag(price_monthly) over(
						partition by account_id
						order by date_month
					)
				, 0) as price_monthly_last_month
		from accounts_monthly
	)

select date_month
	, account_id
	, account_name
	, active_contracts
	, active_contracts_last_month
	, is_paid
	, billing_start_month
	, billing_start_date
	, price_monthly
	, price_monthly_last_month
	, price_monthly - price_monthly_last_month as price_difference_monthly
	, case
		when billing_start_month = date_month
		then 'New'
		when min_active_contracts = 0
		then 'Churned'
		when price_monthly = 0 and price_monthly_last_month > 0
		then 'Churned'
		when price_monthly - price_monthly_last_month > 0
		then 'Expansion'
		when price_monthly - price_monthly_last_month < 0
		then 'Contraction'
		when price_monthly = price_monthly_last_month
		then 'Stable'
		else 'N/A'
	end as mom_variation_type
from accounts_monthly_lag
