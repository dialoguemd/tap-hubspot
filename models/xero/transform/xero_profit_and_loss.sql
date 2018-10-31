with
	expenses as (
		select * from {{ ref('xero_expenses_detailed') }}
	)

select date_trunc('month', date) as date_month
	, account_group
	, account_name
	, account_cost_category
	, sum(line_amount_excl_tax) as amount_excl_tax
from expenses
group by 1,2,3,4
