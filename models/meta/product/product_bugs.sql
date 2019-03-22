with jira_issues as (
        select * from {{ ref( 'jira_issues' ) }}
    )

    , github_issues as (
        select * from {{ ref( 'github_issues' ) }}
    )

    , unioned as (
        select summary as title
            , issue_id::text
            , status
            , issue_type
            , created_at
            , resolved_at as closed_at
        from jira_issues
        where issue_type in ('P2 Bug', 'P1 Bug', 'P3 Bug')
            and project_name = 'Dialogue Product'

        union all

        select title
            , issue_id::text
            , state as status
            , case when priority = 'P1' then 'P1 Bug'
                when priority = 'P2' then 'P2 Bug'
                end as issue_type
            , created_at
            , closed_at
        from github_issues
        where priority in ('P1','P2')
            and is_bug
    )

select title
    , issue_type
    , max(issue_id) as bug_id
    , max(closed_at) as closed_at
    , min(created_at) as created_at
from unioned
group by 1,2
