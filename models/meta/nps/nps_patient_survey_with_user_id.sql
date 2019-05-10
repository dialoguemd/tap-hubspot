with
	nps_survey as (
		select * from {{ ref('delighted_survey_patient') }}
	)

	, users_tmp as (
		select * from {{ ref('scribe_users_detailed') }}
	)

	, users as (
		select email
			, min(user_id) as user_id
		from users_tmp
		group by 1
	)

select nps_survey.survey_id
	, nps_survey.episode_id
	, nps_survey.score
	, nps_survey.category
	, nps_survey.comment_char_length
	, nps_survey.is_testimonial
	, nps_survey.tags
	, nps_survey.comment
	, nps_survey.timestamp
	, nps_survey.date_day
	, nps_survey.date_week
	, nps_survey.date_month
	, nps_survey.updated_at
	, coalesce(nps_survey.user_id, users.user_id) as user_id
from nps_survey
inner join users
	using (email)
