select id::bigint as group_id
    , created_at
    , deleted
    , name as group_name
    , updated_at
    , url
from zendesk.groups
