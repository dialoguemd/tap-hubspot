with
	adjusted_revenue as (
		select * from {{ ref('finance_adjusted_revenue_monthly') }}
	)

select account_id
	, date_month
	, sum(amount) as amount
from adjusted_revenue
group by 1,2
having sum(amount) <> 0
