select id as contact_id
	, name as contact_name
	, email
	, title as contact_title
	, account_id
	, first_name
	, last_name
	, contact_type_c as contact_type
from salesforce.contacts
where not is_deleted
