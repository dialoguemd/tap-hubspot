with
	account_properties as (
		select * from {{ ref('data_xero_account_properties') }}
	)

	, expenses as (
		select * from {{ ref('xero_expenses') }}
	)

	, budget_categories as (
		select * from {{ ref('xero_account_budget_categories') }}
	)

select expenses.*
	, coalesce(account_properties.account_group, 'N/A') as account_group
	, coalesce(budget_categories.account_budget_category, 'N/A') as account_budget_category
from expenses
left join account_properties
	using (account_name)
left join budget_categories
	on expenses.account_name = budget_categories.xero_account_name
