select issue_key
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
from jira.issue_created
