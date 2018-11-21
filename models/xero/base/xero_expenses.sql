select *
	, date as date_day
	, date_trunc('week', date) as date_week
	, date_trunc('month', date) as date_month
from xero.expenses
