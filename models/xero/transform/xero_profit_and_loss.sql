with
	expenses as (
		select * from {{ ref('xero_expenses_detailed') }}
	)

select date_month
	, account_group
	, account_name
	, sum(line_amount_excl_tax) as amount
	, sum(line_amount) as amount_with_tax
from expenses
{{ dbt_utils.group_by(3) }}
