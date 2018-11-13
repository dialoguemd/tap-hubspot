select channel_id as episode_id
	, timestamp as completed_at
	, cd_qnaire_tid as qnaire_tid
	, cd_qnaire as qnaire
	, *
from countdown.questionnaire_finish
