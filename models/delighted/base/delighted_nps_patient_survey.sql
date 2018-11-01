select email
	, user_id
	, episode_id
	, score
	, category
	, tags
	, comment
	, created_at as received_at
from delighted.nps_survey
