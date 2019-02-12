with
	revenue_monthly as (
		select *
		from {{ ref('finance_revenue_adjusted_by_account_monthly_v2') }}
	)

	, accounts as (
		select * from {{ ref('accounts') }}
	)

	, pilot_expansions as (
		select * from {{ ref('finance_pilot_expansions') }}
	)

	, accounts_life_span as (
		select accounts.account_id
			, accounts.account_name
			, accounts.billing_start_date
			, accounts.billing_start_month
			, accounts.is_churned
			, accounts.churn_date
			, accounts.churn_month
			, accounts.last_contract_end_month
			, min(revenue_monthly.date_month) as first_month
			, least(
				-- at most last months since we do not have complete data
				-- for the current month
				date_trunc('month', current_date) - interval '1 month'
				-- at most one month after the account churn date
				-- since we loose revenue until this date
				, accounts.last_contract_end_month + interval '1 month'
			) as last_month
		from revenue_monthly
		inner join accounts
			using (account_id)
		{{ dbt_utils.group_by(n=8) }}
	)

	, accounts_months as (
		select *
			, least(first_month, billing_start_month) as billing_start_month_fixed
			, generate_series(first_month, last_month, interval '1 month')
				as date_month
		from accounts_life_span
	)

	, account_revenue_monthly as (
		select accounts_months.date_month
			, accounts_months.account_id
			, accounts_months.account_name
			, accounts_months.billing_start_date
			, accounts_months.billing_start_month
			, accounts_months.is_churned
			, accounts_months.churn_date
			, accounts_months.churn_month
			, accounts_months.first_month
			, accounts_months.last_month
			, coalesce(revenue_monthly.amount, 0) as amount
			, coalesce(
				lag(revenue_monthly.amount) over(
					partition by accounts_months.account_id
					order by accounts_months.date_month
				)
				, 0) as amount_last_month
			, pilot_expansions.account_id is not null as is_pilot_expansion
		from accounts_months
		left join revenue_monthly
			using (account_id, date_month)
		left join pilot_expansions
			using (account_id, date_month)

	)

select *
	, case
		-- If an account started mid-month, count the first two months
		-- as "New" revenue as the first month will not include the full MRR
		when is_pilot_expansion
		then 'Stable'
		when least(billing_start_month, first_month) = date_month
			or (
				extract('day' from billing_start_date) <> 1
				and billing_start_month = date_month - interval '1 month'
				and (
					first_month = date_month - interval '1 month'
					or first_month = date_month
				)
			)
		then 'New'
		when is_churned
			and churn_month <= date_month
		then 'Churned'
		when amount_last_month < amount
		then 'Expansion'
		when amount_last_month > amount
		then 'Contraction'
		when amount_last_month = amount
		then 'Stable'
	end as amount_variation_type
	, case
		when is_pilot_expansion then 0
		else amount - coalesce(amount_last_month, 0)
	end as amount_variation
	, case
		when least(billing_start_month, first_month) = date_month
		then 'New'
		when churn_month = date_month
		then 'Churned'
		else 'Stable'
	end as logo_variation_type
from account_revenue_monthly
