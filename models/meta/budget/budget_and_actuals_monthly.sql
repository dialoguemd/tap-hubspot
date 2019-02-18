with
	expenses as (
		select * from {{ ref('xero_expenses_detailed') }}
	)

	, budget as (
		select * from {{ ref('xero_budget_monthly_detailed')}}
	)

	, expenses_monthly as (
		select date_month
			, account_budget_category
			, account_group
			, sum(line_amount) as amount_actual
		from expenses
		group by 1,2,3
	)

select expenses_monthly.date_month
	, expenses_monthly.account_budget_category
	, expenses_monthly.account_group
	, round(
		coalesce(budget.amount, 0)::numeric
		,2) as amount_budget
	, round(
		coalesce(expenses_monthly.amount_actual, 0)::numeric
		,2) as amount_actual
	, round(
		(
			coalesce(budget.amount, 0)
			- coalesce(expenses_monthly.amount_actual, 0)
		) :: numeric
		,2) as delta
from expenses_monthly
full outer join budget
	using (date_month, account_budget_category)
-- Budget started being tracked in Oct 2018
where expenses_monthly.date_month >= '2018-10-01'
	and expenses_monthly.account_group <> 'N/A'
