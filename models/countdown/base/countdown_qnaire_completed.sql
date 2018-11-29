select channel_id as episode_id
	, timestamp
	, timezone('America/Montreal', timestamp) as timestamp_est
	, cd_qnaire_tid as qnaire_tid
	, cd_qnaire as qnaire
	, id as event_id
	, row_number() over
		(partition by cd_qnaire_tid order by timestamp desc) as rank
from countdown.questionnaire_finish
