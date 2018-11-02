select id as issue_id
    , number
    , state
    , created_at
    , updated_at
    , closed_at
    , priority
    , feature_affected
    , is_bug
    , source
    , labels
    , pull_request_url
    , assignee
    , issue_user
    , title
    , repo
    , milestone
from github.issues
