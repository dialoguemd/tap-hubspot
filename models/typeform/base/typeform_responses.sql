with
	responses as (
		select *
			, row_number() over(partition by response_id, question_id) as rank
		from typeform.responses
	)

select response_id
	, form_score
	, form_submitted_at
	, form_landed_at
	, question_id
	, question_type
	, answer_type
	, choices_answer
	, free_text_answer
	, number_answer
	, boolean_answer
	, hidden
	, hidden::json->>'user_id' as user_id
	, case
		when (hidden::json->>'org_id') similar to '[0-9]+'
		then (hidden::json->>'org_id')::int
	end as organization_id
	, coalesce(choice_answer
		, free_text_answer
		, boolean_answer::text
		, number_answer::text
	) as answer
from responses
where rank = 1
