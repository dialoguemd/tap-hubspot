with
	opportunities as (
		select * from {{ ref('salesforce_opportunities') }}
	)

	, accounts as (
		select * from {{ ref('salesforce_accounts') }}
	)

	, users as (
		select * from {{ ref('salesforce_users') }}
	)

	, contacts as (
		select * from {{ ref('salesforce_contacts') }}
	)

	, opportunity_product as (
		select * from {{ ref('salesforce_opportunity_product_detailed') }}
	)

	, inbound_lead_sources as (
		select * from {{ ref('salesforce_inbound_lead_sources') }}
	)

	, products as (
		select opportunity_id
			, array_agg(product_id order by product_id) as product_ids
			, array_agg(product_name order by product_name) as product_names

		{% for col in ["list_price", "unit_price", "total_price", "quantity"] %}
			, max(
				case
					when product_id = '01t6A000002NnLXQA0'
					then {{col}}
					else null
				end) as {{col}}_virtual_care
			, max(
				case
					when product_id = '01t6A000002NnLSQA0'
					then {{col}}
					else null
				end) as {{col}}_24_7
			, max(
				case
					when product_id = '01t6A000003s6n0QAA'
					then {{col}}
					else null
				end) as {{col}}_vaccination_campaign
			, max(
				case
					when product_id = '01t6A000002NnLNQA0'
					then {{col}}
					else null
				end) as {{col}}_mental_health
		{% endfor %}

		from opportunity_product
		group by 1
	)

select opportunities.*
	, owners.name as owner_name
	, owners.title as owner_title
	, owners.province as owner_province
	, owners.started_date as owner_started_date
	, accounts.industry
	, accounts.account_name
	, coalesce(
		accounts.billing_state_code,
		owners.state_code
	) as province
	, coalesce(
		accounts.billing_country_code,
		owners.country_code
	) as country
	-- hardcode virtual care if there is no product
	, coalesce(products.product_ids, array['01t6A000002NnLXQA0']) as product_ids
	, coalesce(products.product_names, array['Virtual Care']) as product_names
	, '01t6A000002NnLSQA0' = any (
			coalesce(products.product_ids, array['01t6A000002NnLXQA0'])
	) as includes_24_7
	, '01t6A000003s6n0QAA' = any (
			coalesce(products.product_ids, array['01t6A000002NnLXQA0'])
	) as includes_vaccination_campaign
	, '01t6A000002NnLNQA0' = any (
			coalesce(products.product_ids, array['01t6A000002NnLXQA0'])
	) as includes_mental_health
	, '01t6A000002NnLXQA0' = any (
			coalesce(products.product_ids, array['01t6A000002NnLXQA0'])
	) as includes_virtual_care

	{% for col in ["list_price", "unit_price", "total_price", "quantity"] %}
	, {{col}}_virtual_care
	, {{col}}_24_7
	, {{col}}_vaccination_campaign
	, {{col}}_mental_health
	{% endfor %}

	, partner_contacts.contact_id as partner_contact_id
	, partner_contacts.contact_name as partner_contact_name
	, partners.account_id as partner_account_id
	, partners.account_name as partner_name
	, inbound_lead_sources.lead_source is not null as is_inbound
	, sdr.name as sdr_name
	, sdr.title as sdr_title
from opportunities
inner join accounts
	using (account_id)
left join inbound_lead_sources
	on accounts.account_source = inbound_lead_sources.lead_source
inner join users as owners
	on opportunities.owner_id = owners.user_id
left join products
	on opportunities.opportunity_id = products.opportunity_id
left join contacts as partner_contacts
	on opportunities.partner_individual_id = partner_contacts.contact_id
left join accounts as partners
	on opportunities.partner_id = partners.account_id
left join users as sdr
	on opportunities.sdr_id = sdr.user_id
