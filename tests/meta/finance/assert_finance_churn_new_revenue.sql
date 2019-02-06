with
	finance_churn_monthly as (
		select * from {{ ref('finance_churn_monthly') }}
	)

	, aggregate as (
		select account_id
			, account_name
			, billing_start_date
			, min(date_month) filter(where amount_variation_type = 'New')
				as first_month
			, max(date_month) filter(where amount_variation_type = 'New')
				as last_month
			, extract(epoch from
				max(date_month)  filter(where amount_variation_type = 'New')
				- min(date_month) filter(where amount_variation_type = 'New')
			) / 3600 / 24 / 30 as interval_days
			, count(*) filter(where amount_variation_type = 'New') as new_cnt
		from finance_churn_monthly
		group by 1,2,3
	)

select *
from aggregate
where new_cnt > 2
	-- TODO: add test when Société en Commandite du Lac Beauregard and
	-- Connect&Go are fixed
	-- or (new_cnt = 0 and billing_start_date >= '2017-12-01')
	or interval_days > 31
