with
	qnaire_completions as (
		select * from {{ ref('countdown_qnaire_completion_stats') }}
	)

	, question_replied as (
		select * from {{ ref('countdown_question_replied') }}
	)

	, qnaire_started as (
		select * from {{ ref('countdown_qnaire_started') }}
	)

	, first_reply as (
		select qnaire_started.episode_id
			, qnaire_started.qnaire_tid
			, qnaire_started.timestamp
			, row_number()
				over (partition by qnaire_started.episode_id
					order by qnaire_started.timestamp)
					as rank
			, min(question_replied.reply_labels)
				filter (where question_replied.question_name = 'select_channel')
				as channel_selected
			, min(question_replied.reply_labels)
				filter (where question_replied.question_name = 'appointment_preference')
				as appointment_preference
		from qnaire_started
		left join question_replied
			using (qnaire_tid)
		where question_replied.qnaire_name = 'channel_selection'
		group by 1,2,3
	)

select qnaire_completions.episode_id
	, first_reply.channel_selected
	, first_reply.appointment_preference
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
	, min(qnaire_completions.resume_count)
		filter (where qnaire_completions.qnaire_name = 'dxa')
		-- DXA used to be re-triggered manually instead of resumed
		+ count(distinct qnaire_completions.qnaire_tid)
			filter (where qnaire_completions.qnaire_name = 'dxa')
		- 1
		as dxa_resume_count
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
group by 1,2,3
