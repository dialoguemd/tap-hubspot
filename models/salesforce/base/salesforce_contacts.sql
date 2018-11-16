select id as contact_id
	, name as contact_name
	, email as contact_email
	, title as contact_title
	, account_id
from salesforce.contacts
where not is_deleted
