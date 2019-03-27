with
	episodes as (
		select * from {{ ref('episodes') }}
	)

	, question_ask as (
		select * from {{ ref('countdown_question_asked') }}
	)

	, question_reply as (
		select * from {{ ref('countdown_question_replied') }}
	)

	, qnaire_completed as (
		select * from {{ ref('countdown_qnaire_completed') }}
	)

select question_ask.episode_id
	, episodes.issue_type
	, episodes.outcome
	, question_ask.qnaire_name
	, question_ask.timestamp as started_at
	, question_reply.timestamp as completed_at
	, question_ask.question_tid
	, question_ask.question_name
	, question_ask.user_id
	, question_ask.qnaire_tid
	, question_reply.reply_value
	, extract(epoch from question_reply.timestamp - question_ask.timestamp)
		as response_time
	, question_reply.qnaire_tid is not null as question_answered
	, qnaire_completed.qnaire_tid is not null as questionnaire_completed
from question_ask
left join question_reply
	using (qnaire_tid, question_tid, episode_id)
left join qnaire_completed
	using (qnaire_tid, episode_id)
left join episodes
	using (episode_id)
