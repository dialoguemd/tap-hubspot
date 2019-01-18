with
	responses as (
		select * from {{ ref('typeform_responses_detailed') }}
	)

	, organizations as (
		select * from {{ ref('organizations') }}
	)

	, specialists as (
	    select choices_answer[1] as specialist_name
	        , response_id
	    from responses
	    where question_id in ('DggGJwmci4M6', 'HVzuTJLk2v7d')
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
	, responses.form_title
	, responses.question_title
	, coalesce(specialists.specialist_name, responses.specialist_name)
		as specialist_name
	, organizations.organization_name
	, organizations.account_id
	, organizations.account_name
	, organizations.billing_start_date as launch_date
from responses
left join organizations
	using (organization_id)
left join specialists
	using (response_id)
where
-- exclude identifying questions
-- These questions don't include any useful information
	responses.question_id not in (
		'ZLkkdqEuAthg'
		, 'K2E9T8kHiuJh'
		, '67870899'
		, '67870887'
	) and responses.form_title in (
		'Sondage de satisfaction suite au lancement'
        , 'Post-Onboarding survey'
    )
