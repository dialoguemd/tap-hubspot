with
	opportunity_product as (
		select id as opportunity_product_id
			, opportunity_id
			, product_2_id as product_id
			, quantity
			, list_price::float as list_price
			, name as opportunity_product_name
			, subtotal::float as subtotal
			, description
			, created_by_id
			, created_date
			, pricebook_entry_id
			, total_price::float as total_price
			, last_modified_date
			, unit_price::float as unit_price
			, service_date
			, received_at
			, last_modified_by_id
			, uuid_ts
			, system_modstamp
			, bs_opportunity_product_po_1_c
			, row_number() over (
				partition by concat(opportunity_id, product_2_id)
				order by created_date desc
			) as rank
		from salesforce.opportunity_product
		where not is_deleted
	)

select *
from opportunity_product
where rank = 1
