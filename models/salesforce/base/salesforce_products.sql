select *
	, id as product_id
	, name as product_name
from salesforce.products
where not is_deleted
