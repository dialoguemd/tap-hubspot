with
	replies as (
		select * from {{ ref('dxa_question_replied') }}
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

	, dangerous_flag as (
		select * from {{ ref('dxa_dangerous_symptoms_by_cc') }}
	)

select replies.*
	, episodes.cc_code
	, dangerous_flag.cc_code is not null as flagged_as_dangerous
from replies
left join episodes
	using (episode_id)
left join dangerous_flag
	on replace(replies.question_name, '__yn', '') = dangerous_flag.symptom
	and episodes.cc_code = dangerous_flag.cc_code
-- Only select questions that are booleans
where question_name like '%__yn%'
