with incidents as (
		select * from {{ ref( 'zendesk_incidents_w_problem_detail' ) }}
	)

	, jira_issues as (
		select * from {{ ref( 'jira_issues' ) }}
	)

	, links as (
		select * from {{ ref( 'zendesk_links_current' ) }}
	)

select incidents.problem_id
    , incidents.incident_id
    , incidents.priority
    , incidents.status
    , incidents.type
    , incidents.problem_created_at
    , incidents.reported_at
    , incidents.group_name
    , incidents.tags
    , incidents.feature_tag
    , incidents.os_tag
    , incidents.platform_tag
    , case when links.issue_id is not null
	    then links.issue_id || ' ' || incidents.subject
	    else incidents.subject end as subject
	, CONCAT('https://godialogue.zendesk.com/agent/tickets/',
			 incidents.problem_id
			) as url
	, case when links.issue_id is not null then
	    CONCAT('https://dialoguemd.atlassian.net/browse/',
			links.issue_id
			) end as jira_url
	, case when jira_issues.status = 'Done' then 'Done'
	    when lower(sprint) like '%sprint%'
	        or jira_issues.status = 'In Progress' then 'In Sprint'
	    else jira_issues.status
	    end as issue_status
from incidents
left join links using (problem_id)
left join jira_issues using (issue_id)
where incidents.group_name like '% Support'
