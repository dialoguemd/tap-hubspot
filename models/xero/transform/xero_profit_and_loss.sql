with
	xero_expenses as (
		select *
		from {{ ref('xero_expenses') }}
	)

select date_trunc('month', date) as date_month
	, account_group
	, account_name
	, cost_category
	, sum(line_amount_excl_tax) as amount_excl_tax
from xero_expenses
group by 1,2,3,4
