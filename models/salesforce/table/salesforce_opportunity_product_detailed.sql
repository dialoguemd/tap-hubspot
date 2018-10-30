with
	opportunity_product as (
		select * from {{ ref('salesforce_opportunity_product') }}
	)

	, products as (
		select * from {{ ref('salesforce_products') }}
	)

	, opportunities as (
		select * from {{ ref('salesforce_opportunities') }}
	)

select opportunity_product.*
	, products.product_name
	, opportunities.opportunity_name
from opportunity_product
inner join products
	on opportunity_product.product_id = products.product_id
inner join opportunities
	on opportunity_product.opportunity_id = opportunities.opportunity_id
