with
	responses as (
		select * from {{ ref('typeform_responses') }}
	)

	, questions as (
		select * from {{ ref('typeform_questions') }}
	)

	, organizations_mapping as (
		select * from {{ ref('typeform_organization_mapping') }}
	)

select responses.response_id
	, responses.form_score
	, responses.form_submitted_at
	, responses.form_landed_at
	, responses.question_id
	, responses.question_type
	, responses.answer_type
	, responses.choices_answer
	, responses.free_text_answer
	, responses.number_answer
	, responses.boolean_answer
	, responses.hidden
	, responses.user_id
	, responses.answer
	, coalesce(organizations_mapping.organization_id
		, responses.organization_id
		) as organization_id
	, organizations_mapping.specialist_name
	, questions.form_title
	, questions.question_title
from responses
inner join questions
	using (question_id)
left join organizations_mapping
	using (response_id)
