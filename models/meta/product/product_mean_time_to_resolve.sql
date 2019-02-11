with jira_issues as (
        select * from {{ ref('jira_issues') }}
    )

    , working_minutes as (
        select * from {{ ref('dimension_working_minutes') }}
    )

    , weeks as (
        select * from {{ ref('dimension_weeks_saturday_start') }}
    )

    , issues as (
        select jira_issues.issue_id
            , jira_issues.issue_type
            , jira_issues.resolved_at
            , case
                when extract ('dow' from jira_issues.resolved_at) > 5
                    then date_trunc('day', jira_issues.resolved_at)
                        - (extract ('dow' from jira_issues.resolved_at) -6)
                        * interval '1 days'
                    else date_trunc('day', jira_issues.resolved_at)
                        - (extract ('dow' from jira_issues.resolved_at) +1)
                        * interval '1 days'
                    end as date_week
            , count(working_minutes.date_minute) as time_to_resolve_working_min
            , count(working_minutes.date_minute) / 480 as time_to_resolve_working_days
        from jira_issues
        left join working_minutes
            on working_minutes.date_minute between jira_issues.created_at
                and coalesce(jira_issues.resolved_at, current_date)
        where jira_issues.issue_type in ('P1 Bug', 'P2 Bug')
        group by 1,2,3
    )

select weeks.date_week
    , count(issues.issue_id) filter (where issue_type = 'P1 Bug')
        as p1_bugs_count
    , count(issues.issue_id) filter (where issue_type = 'P2 Bug')
        as p2_bugs_count
    , sum(time_to_resolve_working_min) filter (where issue_type  = 'P1 Bug')
        as p1_ttr_working_min
    , sum(time_to_resolve_working_min) filter (where issue_type  = 'P2 Bug')
        as p2_ttr_working_min
    , sum(time_to_resolve_working_days) filter (where issue_type  = 'P1 Bug')
        as p1_ttr_working_day
    , sum(time_to_resolve_working_days) filter (where issue_type  = 'P2 Bug')
        as p2_ttr_working_day
from weeks
left join issues
    using (date_week)
group by 1
