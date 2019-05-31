with
	expenses as (
		select * from {{ ref('xero_expenses') }}
	)

	, budget_categories as (
		select * from {{ ref('xero_account_budget_categories') }}
	)

select expenses.*
	, case
		when account_name like 'Health Platform - %' then 'Health Platform'
		when account_name = 'MH Cost' then 'Mental Health'
		when account_name like 'Physical Clinic - %'
			or account_name like 'Physical Clinic Other - %'
		then 'Physical Clinic'
		when account_name like 'Customer Success - %' then 'Customer Success'
		when account_name like 'G&A - %' then 'General Administration'
		when account_name like 'Health team - %'
			or account_name like 'Health Team medical operations - %'
			or account_name = 'Health team medical QA Consulting'
		then 'Health Team'
		when account_name like 'HR - %' then 'Human Ressources'
		when account_name like 'Marketing - %' then 'Marketing Expenses'
		when account_name like 'New Services - %' then 'New Services'
		when account_name like 'Partnerships - %' then 'Partnerships'
		when account_name like 'Sales - %' then 'Sales Expenses'
		when account_name like 'Product & Tech - %'
			or account_name like 'Tech%'
		then 'Technology'
		else 'N/A'
	end as account_group
	, coalesce(budget_categories.account_budget_category, 'N/A') as account_budget_category
from expenses
left join budget_categories
	on expenses.account_name = budget_categories.xero_account_name
