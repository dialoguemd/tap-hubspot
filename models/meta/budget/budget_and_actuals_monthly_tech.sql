with
	budget_and_actuals as (
		select * from {{ ref('budget_and_actuals_monthly') }}
	)

select * from budget_and_actuals
where account_group = 'Technology'
