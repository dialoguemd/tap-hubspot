with
	adjusted_revenue as (
		select * from {{ ref('finance_adjusted_revenue_monthly') }}
	)

	, adjusted_revenue_by_account as (
		select account_id
			, date_month
			, sum(amount) as amount
		from adjusted_revenue
		group by 1,2
		having sum(amount) <> 0
	)

select account_id
	, date_month
	, case
		-- fix for BNC temporary churn
		when account_id = '0016A000005UIH2QAO'
			and date_month in ('2018-05-01', '2018-06-01')
		then 8731.88
		else amount
	end as amount
from adjusted_revenue_by_account
