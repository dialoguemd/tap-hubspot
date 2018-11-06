with
	organizations_monthly as (
		select * from {{ ref('scribe_organizations_monthly') }}
	)

	, sf_scribe_organizations as (
		select * from {{ ref('salesforce_scribe_organizations_detailed') }}
	)

	, accounts_monthly as (
		select organizations_monthly.date_month
			, coalesce(sf_scribe_organizations.account_id
				, 'scribe:' || organizations_monthly.organization_id) as account_id
			, coalesce(sf_scribe_organizations.account_name
					, organizations_monthly.organization_name) as account_name
			, bool_or(is_paid) as is_paid
			, min(billing_start_date) as billing_start_date
			, sum(organizations_monthly.paid_employees) as paid_employees
			-- if all orgs of an account had 0 employees
			-- at some point in the month then the account in churned
			, max(organizations_monthly.min_paid_employees) as min_paid_employees
			, sum(organizations_monthly.price_monthly) as price_monthly
		from organizations_monthly
		left join sf_scribe_organizations
			using (organization_id)
		group by 1,2,3
	)

	, accounts_monthly_lag as (
		select date_month
			, account_id
			, account_name
			, is_paid
			, billing_start_date
			, paid_employees
			, price_monthly
			, min_paid_employees
			, coalesce(
					lag(min_paid_employees) over(
						partition by account_id
						order by date_month
					)
				, 0) as min_paid_employees_last_month
			, coalesce(
					lag(paid_employees) over(
						partition by account_id
						order by date_month
					)
				, 0) as paid_employees_last_month
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
	, paid_employees
	, paid_employees_last_month
	, is_paid
	, billing_start_date
	, price_monthly
	, price_monthly_last_month
	, price_monthly - price_monthly_last_month as price_difference_monthly
	, case
		when price_monthly_last_month = 0 or min_paid_employees_last_month = 0
		then 'New'
		when min_paid_employees = 0
		then 'Churned'
		when price_monthly = 0 and price_monthly_last_month > 0
		then 'Churned'
		when price_monthly - price_monthly_last_month > 0
		then 'Expansion'
		when price_monthly - price_monthly_last_month < 0
		then 'Contraction'
		when price_monthly = price_monthly_last_month
		then 'No change'
		else 'N/A'
	end as mom_variation_type
from accounts_monthly_lag
