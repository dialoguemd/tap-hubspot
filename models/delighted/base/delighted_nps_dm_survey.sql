select email
	, organization_id
	, score
	, category
	, tags
	, comment
	, created_at as timestamp
from delighted.nps_survey_dm
