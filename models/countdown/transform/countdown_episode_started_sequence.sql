with qnaire_completions as (
        select * from {{ ref('countdown_qnaire_completion_stats') }}
    )

	, question_replies as (
		select * from {{ ref('countdown_question_replied') }}
	)

	, first_reply as (
		select episode_id
			, reply_labels as channel_selected
			, row_number()
				over (partition by episode_id order by replied_at)
					as rank
		from question_replies
		where question_replies.qnaire_name = 'channel_selection'
	)

select qnaire_completions.episode_id
	, first_reply.channel_selected
	-- Top Level
	, min(qnaire_completions.started_at_est)
		filter (where qnaire_completions.qnaire_name = 'top_level_greeting')
			as top_level_started_at
	, min(qnaire_completions.completed_at_est)
		filter (where qnaire_completions.qnaire_name = 'top_level_greeting')
			as top_level_completed_at
	-- DXA
	, min(qnaire_completions.started_at_est)
		filter (where qnaire_completions.qnaire_name = 'dxa')
			as dxa_started_at
	, min(qnaire_completions.completed_at_est)
		filter (where qnaire_completions.qnaire_name = 'dxa')
			as dxa_completed_at
	-- Channel Selection
	, min(qnaire_completions.started_at_est)
		filter (where qnaire_completions.qnaire_name = 'channel_selection')
			as channel_select_started_at
	, min(qnaire_completions.completed_at_est)
		filter (where qnaire_completions.qnaire_name = 'channel_selection')
			as channel_select_completed_at
from qnaire_completions
left join first_reply
	on qnaire_completions.episode_id = first_reply.episode_id
	and rank = 1
group by 1,2
