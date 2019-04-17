select issue_key
	, issue_key as issue_id
	, status
	, timestamp
	, date_trunc('day', timestamp) as date_day
	, created_at
	, issue_type
	, squad
	, sprint
	, summary
	, discovered_by
	, description
	, project_name
	, incident_count::integer
	, feature
	, resolved_at
	, resolution
from jira.issue_updated
