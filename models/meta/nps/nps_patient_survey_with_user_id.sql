with
	nps_survey as (
		select * from {{ ref('delighted_nps_patient_survey') }}
	)

	, users as (
		select * from {{ ref('scribe_users') }}
	)

select nps_survey.episode_id
	, nps_survey.score
	, nps_survey.category
	, nps_survey.tags
	, nps_survey.comment
	, nps_survey.timestamp
	, users.user_id
from nps_survey
inner join users
	using (email)
