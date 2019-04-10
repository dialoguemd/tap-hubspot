with
	expenses as (
		select * from {{ ref('xero_expenses_detailed') }}
	)

select date_trunc('month', date) as date_month
	, account_group
	, account_name
	, sum(line_amount) as amount
from expenses
{{ dbt_utils.group_by(3) }}
