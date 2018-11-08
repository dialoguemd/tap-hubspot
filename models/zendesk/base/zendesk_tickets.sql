select id::bigint as ticket_id
    , received_at
    , assignee_id
    , collaborator_ids
    , timezone('America/Montreal', created_at) as created_at
    , description
    , group_id
    , organization_id as zendesk_organization_id
    , priority
    , requester_id
    , status
    , subject
    , submitter_id
    , tags
    , type
    , updated_at
    , url
    , recipient
    , problem_id
from zendesk.tickets
