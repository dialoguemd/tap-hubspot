with
	nps_survey as (
		select * from {{ ref('delighted_nps_patient_survey') }}
	)

	, users as (
		select * from {{ ref('scribe_users') }}
	)

select episode_id
	, score
	, category
	, tags
	, comment
	, received_at
	, users.user_id
from nps_survey
inner join users
	on nps_survey.email = users.email
