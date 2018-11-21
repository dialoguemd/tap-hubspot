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
			, sum(line_amount) as amount_actual
		from expenses
		group by 1,2
	)

select budget.date_month
	, budget.account_budget_category
	, budget.budget_department
	, coalesce(budget.amount, 0) as amount_budget
	, coalesce(expenses_monthly.amount_actual, 0) as amount_actual
from budget
left join expenses_monthly
	using (date_month, account_budget_category)
