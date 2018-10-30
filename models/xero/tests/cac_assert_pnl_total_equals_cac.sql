with
	profit_and_loss as (
		select date_month
			, sum(amount_excl_tax) as cost_total
		from {{ ref('xero_profit_and_loss')}}
		where account_group in ('Sales Expenses', 'Marketing Expenses')
		group by 1
	),

	cac as (
		select *
		from {{ ref('xero_cac')}}
	)

select profit_and_loss.date_month as pl_date_month
	, profit_and_loss.cost_total as pl_cost_total
	, cac.date_month as cac_date_month
	, cac.cost_total as cac_cost_total
from profit_and_loss
full outer join cac
	on profit_and_loss.date_month = cac.date_month
		and profit_and_loss.cost_total::int = cac.cost_total::int
where cac.date_month is null
	or profit_and_loss is null
