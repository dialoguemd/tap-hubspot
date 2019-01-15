with jira_issues as (
        select * from {{ ref('jira_issues') }}
    )

    , working_minutes as (
        select * from {{ ref('dimension_working_minutes') }}
    )

    , weeks as (
        select * from {{ ref('dimension_weeks') }}
    )

    , issues as (
        select jira_issues.issue_id
            , jira_issues.issue_type
            , jira_issues.resolved_at
            , date_trunc('week', resolved_at) as date_week
            , count(working_minutes.minute) as time_to_resolve_working_min
            , count(working_minutes.minute) / 480 as time_to_resolve_working_days
        from jira_issues
        left join working_minutes
            on working_minutes.minute between jira_issues.created_at
                and coalesce(jira_issues.resolved_at, current_date)
        where jira_issues.issue_type in ('P1 Bug', 'P2 Bug')
            and jira_issues.resolved_at
                < date_trunc('week', current_date) - interval '1 day'
        group by 1,2,3
    )

select weeks.date_week
    , count(issues.issue_id) filter (where issue_type = 'P1 Bug') as p1_bugs_count
    , count(issues.issue_id) filter (where issue_type = 'P2 Bug') as p2_bugs_count
    , sum(time_to_resolve_working_min) filter (where issue_type  = 'P1 Bug') as p1_ttr_working_min
    , sum(time_to_resolve_working_min) filter (where issue_type  = 'P2 Bug') as p2_ttr_working_min
    , sum(time_to_resolve_working_days) filter (where issue_type  = 'P1 Bug') as p1_ttr_working_day
    , sum(time_to_resolve_working_days) filter (where issue_type  = 'P2 Bug') as p2_ttr_working_day
from weeks
left join issues
    using (date_week)
group by 1
