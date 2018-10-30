select *
	, product_2_id as product_id
from salesforce.opportunity_product
where not is_deleted
