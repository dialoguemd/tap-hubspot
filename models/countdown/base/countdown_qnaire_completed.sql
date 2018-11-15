select channel_id as episode_id
	, timestamp as completed_at
	, cd_qnaire_tid as qnaire_tid
	, cd_qnaire as qnaire
	, row_number() over
		(partition by cd_qnaire_tid order by timestamp desc) as rank
	, *
from countdown.questionnaire_finish
