with
	eae_comp as (
		select * from {{ ref('eae_compensation_percentage_monthly') }}
	)

	, xero_profit_and_loss as (
		select * from {{ ref('xero_profit_and_loss') }}
	)

select xero_profit_and_loss.date_month
	, sum(xero_profit_and_loss.amount
		* eae_comp.eae_compensation_percentage
	) as cost_eae
	, sum(xero_profit_and_loss.amount
		*  (1-eae_comp.eae_compensation_percentage)
	) as cost_ae
	, sum(xero_profit_and_loss.amount) as cost_total
from xero_profit_and_loss
inner join eae_comp
	using (date_month)
where xero_profit_and_loss.account_group in ('Sales Expenses', 'Marketing Expenses')
group by 1
