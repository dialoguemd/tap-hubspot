with
	completions as (
		select channel_id as episode_id
			, timestamp
			, timezone('America/Montreal', timestamp) as timestamp_est
			, cd_qnaire_tid as qnaire_tid
			, cd_qnaire as qnaire
			, id as event_id
			, cd_q_tid as question_tid
			, cd_q_id as question_name
			, row_number() over (partition by cd_qnaire_tid order by timestamp) as rank
		from countdown.questionnaire_pause
	)

select qnaire_tid
	, question_tid
    , episode_id
    , timestamp
    , timestamp_est
    , qnaire
    , qnaire as qnaire_name
    , event_id
    , question_name
from completions
where rank = 1
