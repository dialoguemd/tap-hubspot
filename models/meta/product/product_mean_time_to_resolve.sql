with jira_issues as (
        select * from {{ ref('jira_issues') }}
    )

    , working_minutes as (
        select * from {{ ref('dimension_working_minutes') }}
    )

select jira_issues.created_at
    , jira_issues.issue_id
    , jira_issues.issue_type
    , jira_issues.resolved_at
    , count(working_minutes.minute) as time_to_resolve_working_min
    , count(working_minutes.minute) / 480 as time_to_resolve_working_days
from jira_issues
left join working_minutes 
    on working_minutes.minute between jira_issues.created_at
    	and coalesce(jira_issues.resolved_at, current_date)
where jira_issues.issue_type in ('P1 Bug', 'P2 Bug')
    and jira_issues.resolved_at
    	< date_trunc('week', current_date) - interval '1 day'
group by 1,2,3,4
