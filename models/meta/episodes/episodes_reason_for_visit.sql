with
	question_replied as (
		select * from {{ ref('countdown_question_replied') }}
	)

	, reason as (
		select episode_id
			, reply_label_first as reason_for_visit
			, row_number() over (partition by episode_id order by replied_at)
				as rank
		from question_replied
		where qnaire_name = 'episode_subject_and_reason'
			and reply_label_first is not null
			and question_name in
				(
					'chat_topic_closed',
					'chat_topic_full_scope',
					'chat_topic_reduced_scope'
				)
	)

select episode_id
	, reason_for_visit
from reason
where rank = 1
