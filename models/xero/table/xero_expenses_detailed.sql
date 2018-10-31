with
	account_properties as (
		select * from {{ ref('data_xero_account_properties') }}
	)

	, expenses as (
		select * from {{ ref('xero_expenses')}}
	)

select expenses.*
	, coalesce(account_properties.account_group, 'N/A') as account_group
	, coalesce(account_properties.account_cost_category, 'N/A') as account_cost_category
from expenses
left join account_properties
	using (account_name)
