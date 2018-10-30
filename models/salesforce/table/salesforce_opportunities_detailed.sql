with
	opportunities as (
		select *
		from {{ ref('salesforce_opportunities') }}
	)

	, accounts as (
		select *
		from {{ ref('salesforce_accounts') }}
	)

	, users as (
		select *
		from {{ ref('salesforce_users') }}
	)

	, products as (
		select opportunity_id
			, array_agg(product_id) as product_ids
			, array_agg(product_name) as product_names
		from {{ ref('salesforce_opportunity_product_detailed') }}
		group by 1
	)

select opportunities.*
	, users.name as owner_name
	, users.title as owner_title
	, users.province as owner_province
	, users.started_date as owner_started_date
	, accounts.industry
	, accounts.account_name
	, coalesce(
		accounts.billing_state_code,
		users.state_code
	) as province
	, coalesce(
		accounts.billing_country_code,
		users.country_code
	) as country
	-- hardcode virtual care if there is no product
	, coalesce(products.product_ids, array['01t6A000002NnLXQA0']) as products_ids
	, coalesce(products.product_names, array['Virtual Care']) as products_names
from opportunities
inner join accounts
	using (account_id)
inner join users
	on opportunities.owner_id = users.user_id
left join products
	on opportunities.opportunity_id = products.opportunity_id
