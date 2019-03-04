with
	completions as (
		select channel_id as episode_id
			, timestamp
			, timezone('America/Montreal', timestamp) as timestamp_est
			, cd_qnaire_tid as qnaire_tid
			, cd_qnaire as qnaire
			, id as event_id
			, user_id
			, row_number() over (partition by cd_qnaire_tid order by timestamp) as rank
		from countdown.questionnaire_finish
	)

select qnaire_tid
    , episode_id
    , timestamp
    , timestamp_est
    , qnaire
    , event_id
    , user_id
from completions
where rank = 1
