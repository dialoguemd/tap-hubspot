with issue_created as (
        select * from {{ ref( 'jira_issue_created' ) }}
    ) 

    , issue_updated as (
        select * from {{ ref( 'jira_issue_updated' ) }}
    )

    , coalesced as (
        select coalesce(issue_updated.issue_key, issue_created.issue_key) as issue_key
            , coalesce(issue_updated.status, issue_created.status) as status
            , coalesce(issue_updated.timestamp, issue_created.timestamp) as timestamp
            , coalesce(issue_updated.created_at, issue_created.created_at) as created_at
            , coalesce(issue_updated.issue_type, issue_created.issue_type) as issue_type
            , coalesce(issue_updated.squad, issue_created.squad) as squad
            , coalesce(issue_updated.sprint, issue_created.sprint) as sprint
            , coalesce(issue_updated.summary, issue_created.summary) as summary
            , coalesce(issue_updated.description, issue_created.description) as description
            , coalesce(issue_updated.project_name, issue_created.project_name) as project_name
            , issue_updated.resolved_at
        from issue_created
        full outer join issue_updated
            on issue_created.issue_key = issue_updated.issue_key
    )

    , ranked as (
        select *
            , row_number() over (partition by issue_key order by timestamp desc) as rank
        from coalesced
    )

select issue_key::text as issue_id
    , status
    , timestamp as updated_at
    , created_at
    , issue_type
    , squad
    , sprint
    , summary
    , description
    , project_name
    , resolved_at
from ranked
where rank = 1
