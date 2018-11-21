with
	budget as (
		select * from {{ ref('xero_budget_monthly') }}
	)

	, departments as (
		select * from {{ ref('xero_budget_departments') }}
	)

select *
from budget
left join departments
	using (account_budget_category)
