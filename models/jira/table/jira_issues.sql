with issue_created as (
        select * from {{ ref( 'jira_issue_created' ) }}
    ) 

    , issue_updated as (
        select * from {{ ref( 'jira_issue_updated' ) }}
    )

    , coalesced as (
        select
        {% for column in ['issue_key',
            'status',
            'timestamp',
            'created_at',
            'issue_type',
            'squad',
            'sprint',
            'summary',
            'discovered_by',
            'description',
            'project_name']
        %}
            coalesce(issue_updated.{{column}}, issue_created.{{column}}) as {{column}},
        {% endfor %}
            issue_updated.resolved_at
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
    , discovered_by
    , description
    , project_name
    , resolved_at
    , issue_type in ('P1 Bug', 'P2 Bug', 'P3 Bug') as is_bug
from ranked
where rank = 1
