with
	products as (
		select * from {{ ref('salesforce_opportunity_product_detailed') }}
	)

select date_trunc('quarter', close_date) as date_quarter
	, date_trunc('week', close_date) as date_week
	, coalesce(
		sum(subtotal) filter(
			where product_name = 'Stress Management & Well-Being'
		)
		, 0
	) as mrr_stress_mgmt
	, coalesce(
		sum(subtotal) filter(where product_name = '24/7')
		, 0
	) as mrr_24_7
from products
where is_won
group by 1,2
