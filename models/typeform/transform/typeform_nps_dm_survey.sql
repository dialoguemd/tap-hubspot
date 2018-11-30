with
	responses as (
		select * from {{ ref('typeform_responses_detailed') }}
	)

	, questions as (
		select * from {{ ref('typeform_questions') }}
	)

select ''::text as email
	, number_answer as score
	, case
		when number_answer >= 9 then 'promoter'
		when number_answer <= 6 then 'detractor'
		else 'passive'
	end as category
	, ''::text as comment
	, form_submitted_at as timestamp
	, form_submitted_at as updated_at
	, ''::text as contact_type
	, organization_id
	, null::text as delighted_workspace
	, null::text[] as tags
	, 0 as month_since_billing_start_date
from questions
inner join responses
	using (question_id)
where questions.question_id in ('Zj5euVUa95Gn', '67871435')
