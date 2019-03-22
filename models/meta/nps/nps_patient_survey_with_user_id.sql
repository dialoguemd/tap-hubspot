with
	nps_survey as (
		select * from {{ ref('delighted_nps_patient_survey') }}
	)

	, users as (
		select * from {{ ref('scribe_users_detailed') }}
	)

select md5(coalesce(nps_survey.email) || nps_survey.timestamp::text) as survey_id
	, nps_survey.episode_id
	, nps_survey.score
	, nps_survey.category
	, nps_survey.tags
	, nps_survey.comment
	, nps_survey.timestamp
	, nps_survey.updated_at
	, users.user_id
from nps_survey
inner join users
	using (email)
