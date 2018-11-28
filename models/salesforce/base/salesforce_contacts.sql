select id as contact_id
	, name as contact_name
	, email
	, title as contact_title
	, account_id
	, contact_type_c as contact_type
	, case
		-- Only include names that are more than 3 characters
		when first_name similar to '[a-zA-zéè]{3,}'
		then initcap(first_name)
		else ''
	end as first_name
	, last_name
from salesforce.contacts
where not is_deleted
