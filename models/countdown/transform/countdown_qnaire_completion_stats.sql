with qnaire_started as (
		select * from {{ ref( 'countdown_qnaire_started' ) }}
	)

	, qnaire_completed as (
		select * from {{ ref( 'countdown_qnaire_completed' ) }}
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
from qnaire_started
left join qnaire_completed using (qnaire_tid)
