select email
	, organization_id
	, score
	, category
	, tags
	, comment
	, created_at as timestamp
	, updated_at
	, contact_type
	, month_since_billing_start_date
	, delighted_workspace
from delighted.nps_survey_dm
