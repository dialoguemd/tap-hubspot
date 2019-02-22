with
	question_asked as (
		select * from {{ ref('countdown_question_asked') }}
	)

	, question_replied as (
		select * from {{ ref('countdown_question_replied') }}
	)

	, qnaire_paused as (
		select * from {{ ref('countdown_qnaire_paused') }}
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

	, questions as (
		select question_asked.question_tid
			, question_asked.qnaire_tid
			, question_asked.user_id
			, question_asked.episode_id
			, question_asked.qnaire_name
			, question_asked.question_name
			, question_asked.timestamp as asked_at
			, question_replied.replied_at
			, question_replied.reply_labels
			, question_replied.reply_value
			, question_replied.reply_values
			, case when qnaire_paused.qnaire_tid is not null then 'interrupted'
				when question_replied.qnaire_tid is not null then 'replied'
				else 'unanswered' end as response_type
			, row_number()
				over (partition by question_asked.qnaire_tid order by question_asked.timestamp)
				as question_rank
		from question_asked
		left join question_replied
			using(qnaire_tid, question_tid)
		left join qnaire_paused
			using(qnaire_tid, question_tid)
		where question_asked.qnaire_name = 'dxa'
	)

select questions.episode_id
	, questions.question_tid
	, questions.qnaire_tid
	, questions.question_name as question
	, questions.question_rank
	, questions.response_type
	, questions.asked_at
	, questions.replied_at
	, episodes.language
	, episodes.gender
	, episodes.age
	, episodes.issue_type
	, episodes.outcome
	, episodes.cc_code
    , episodes.reason_for_visit
    , episodes.cc_label_en
	, extract(epoch from (questions.replied_at - questions.asked_at))
		as response_time
	,lower(
		replace(
			replace(
				replace(questions.question_name, 'iQR_', '')
			, ' ', '')
		, '.', '')) as question_name
	, lower(
		replace(
			replace(questions.reply_labels,'["', ''),
		 '"]', '')) as reply_value
from questions
left join episodes
	using (episode_id)
