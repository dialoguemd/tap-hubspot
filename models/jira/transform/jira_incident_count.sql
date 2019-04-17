with
    issue_created as (
        select * from {{ ref('jira_issue_created') }}
    ) 

    , issue_updated as (
        select * from {{ ref('jira_issue_updated') }}
    )

    , issues as (
        select * from {{ ref('jira_issues') }}
    )

    , incidents_added as (
        select date_day
            , issue_id
            , max(incident_count) as incident_count
        from issue_updated
        where incident_count > 0
        group by 1,2
    )

    , resolves as (
        select issue_id
            , max(resolved_at) as resolved_at
        from issue_updated
        group by 1
    )

    , daily as (
        select issue_created.issue_id
            , issue_created.summary
            , issue_created.created_at
            , issue_created.issue_type
            , issue_created.description
            , issue_created.incident_count
            , generate_series(date_trunc('day', issue_created.created_at),
                date_trunc('day', coalesce(resolves.resolved_at, current_timestamp)),
                '1 day') as date_day
        from issue_created
        left join resolves
            using (issue_id)
        where issue_created.issue_type in
            ('Sub-bug',
            'Polish',
            'P2 Bug',
            'P1 Bug',
            'P3 Bug')
    )

    , daily_with_incidents as (
        select daily.date_day
            , daily.issue_id
            , daily.summary
            , daily.created_at
            , daily.issue_type
            , daily.description
            , max(coalesce(incidents_added.incident_count,0))
                over (partition by daily.issue_id order by daily.date_day
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                as incident_count
        from daily
        left join incidents_added
            using (issue_id, date_day)
    )
        
select daily_with_incidents.*
    , issues.feature
    , issues.resolution
    , issues.discovered_by
    , issues.resolved_at
    , issues.resolved_at is not null as is_resolved
    , coalesce(
        lag(daily_with_incidents.incident_count)
            over (partition by daily_with_incidents.issue_id
                order by daily_with_incidents.date_day)
        , 0)
        as previous_day_incident_count
    , daily_with_incidents.incident_count
        - coalesce(lag(daily_with_incidents.incident_count)
        over (partition by daily_with_incidents.issue_id
            order by daily_with_incidents.date_day)
        , 0)
        as new_incidents_count
from daily_with_incidents
-- Join with issues to get up to date dimension info for the issue
left join issues
    using (issue_id)
