select email
	, user_id
	, episode_id
	, score
	, category
	, tags
	, comment
	, created_at as timestamp
from delighted.nps_survey
