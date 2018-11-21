select date_month
	, account_budget_category
	, amount::float as amount
from {{ ref('data_xero_budget_monthly') }}
