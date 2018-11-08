select created_at
    , id as link_id
    , issue_key::text as issue_id
    , ticket_id as problem_id
    , updated_at
    , url
    , row_number()
		over (partition by ticket_id order by created_at desc)
		as rank
from zendesk.links
