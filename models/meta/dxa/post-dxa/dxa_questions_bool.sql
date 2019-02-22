with
	replies as (
		select * from {{ ref('dxa_questions') }}
	)

	, dangerous_flag as (
		select * from {{ ref('dxa_dangerous_symptoms_by_cc') }}
	)

select replies.*
	, dangerous_flag.cc_code is not null as flagged_as_dangerous
from replies
left join dangerous_flag
	on replace(replies.question_name, '__yn', '') = dangerous_flag.symptom
	and replies.cc_code = dangerous_flag.cc_code
-- Only select questions that are booleans
where question_name like '%__yn%'
