with replies as (
        select * from {{ ref('dxa_questions') }}
    )

select episode_id
	, qnaire_tid
	, question_tid
	, left(reply_value, (position('__free' in reply_value)-1)) as question_name
	-- Symptoms Duration
	, case when question_name like '%debut_symptome__free%'
		then reply_value
		else null end as symptoms_duration
	-- Menses Duration
	, case when question_name like '%ddm__free%'
		then reply_value
		else null end as menses_duration
from replies
