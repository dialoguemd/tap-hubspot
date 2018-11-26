select email
	, user_id
	, episode_id
	, score
	, category
	, tags
	, comment
	, created_at as timestamp
	, date_trunc('month', created_at) as date_month
	, date_trunc('week', created_at) as date_week
from delighted.nps_survey
