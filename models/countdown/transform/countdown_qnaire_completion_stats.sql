with qnaire_started as (
		select * from {{ ref( 'countdown_qnaire_started' ) }}
	)

	, qnaire_completed as (
		select * from {{ ref( 'countdown_qnaire_completed' ) }}
	)

select qnaire_started.episode_id
  , qnaire_started.qnaire
  , qnaire_started.started_at
  , qnaire_completed.completed_at
  , qnaire_started.user_id
  , qnaire_started.qnaire_tid
  , extract(epoch from qnaire_completed.completed_at - qnaire_started.started_at)
  	as questionnaire_completion_time
  , qnaire_completed.id is not null as questionnaire_completed
from qnaire_started
left join qnaire_completed using (qnaire_tid)
