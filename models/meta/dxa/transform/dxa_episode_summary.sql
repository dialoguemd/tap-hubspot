with
    invocations as (
        select * from {{ ref('dxa_invocations') }}
    )

select episode_id
	, user_id
	, age
	, gender
	, language
	, outcome
	, issue_type
	, reason_for_visit
	, cc_code
	, min(started_at) as started_at
	, max(completed_at) as completed_at
	, bool_or(questionnaire_completed) as dxa_completed
	, count(qnaire_tid) as invocations_count
	, extract(epoch from max(completed_at) - min(started_at))
		as completion_time
from invocations
{{ dbt_utils.group_by(9) }}
