select channel_id as episode_id
	, timestamp
	, timezone('America/Montreal', timestamp) as timestamp_est
	, cd_qnaire_tid as qnaire_tid
	, cd_qnaire as qnaire
	, user_id
	, id as event_id
from countdown.questionnaire_start
