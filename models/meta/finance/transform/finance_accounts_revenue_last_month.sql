with
	account_revenue_monthly as (
		select *
		from {{ ref('finance_revenue_adjusted_by_account_monthly_v2') }}
	)

	, account_revenue_ranked as (
		select *
			, row_number()
				over(
					partition by account_id
					order by date_month desc
				) as rank
		from account_revenue_monthly
	)

select account_id
	, date_month as last_billed_month
	, amount
from account_revenue_ranked
where rank = 1
