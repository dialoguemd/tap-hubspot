with
	responses as (
		select * from {{ ref('typeform_responses_detailed') }}
	)

	, organizations as (
		select * from {{ ref('organizations') }}
	)

select responses.*
	, organizations.organization_name
	, organizations.account_id
	, organizations.account_name
from responses
left join organizations
	using (organization_id)
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
