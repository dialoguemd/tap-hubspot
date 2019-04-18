with
	qnaire_started as (
		select * from {{ ref('countdown_qnaire_started') }}
	)

	, qnaire_completed as (
		select * from {{ ref('countdown_qnaire_completed') }}
	)

	, qnaire_resumed as (
		select * from {{ ref('countdown_qnaire_resumed') }}
	)

	, qnaire_resumed_summary as (
		select qnaire_tid
			, min(timestamp) as first_resumed_at
			, min(timestamp_est) as first_resumed_at_est
			, max(timestamp) as last_resumed_at
			, max(timestamp_est) as last_resumed_at_est
			, count(*) as resume_count
		from qnaire_resumed
		group by 1
	)

select qnaire_started.episode_id
	, qnaire_started.qnaire
	, qnaire_started.qnaire_name
	, qnaire_started.timestamp as started_at
	, qnaire_completed.timestamp as completed_at
	, qnaire_started.timestamp_est as started_at_est
	, qnaire_completed.timestamp_est as completed_at_est
	, qnaire_started.user_id
	, qnaire_started.qnaire_tid
	, extract(epoch from qnaire_completed.timestamp - qnaire_started.timestamp)
		as questionnaire_completion_time
	, qnaire_completed.event_id is not null as questionnaire_completed
	, qnaire_resumed_summary.qnaire_tid is not null as questionnaire_resumed
	, coalesce(qnaire_resumed_summary.resume_count, 0) as resume_count
	, qnaire_resumed_summary.first_resumed_at
	, qnaire_resumed_summary.first_resumed_at_est
	, qnaire_resumed_summary.last_resumed_at
	, qnaire_resumed_summary.last_resumed_at_est
from qnaire_started
left join qnaire_completed
	using (qnaire_tid)
left join qnaire_resumed_summary
	using (qnaire_tid)
