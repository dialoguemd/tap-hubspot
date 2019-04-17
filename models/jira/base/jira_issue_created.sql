select issue_key
	, issue_key as issue_id
	, status
	, timestamp
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
from jira.issue_created
