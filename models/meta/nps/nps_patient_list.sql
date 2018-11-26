with
	nps_survey as (
		select * from {{ ref('nps_patient_survey') }}
	)

	, posts_daily as (
		select * from {{ ref('messaging_posts_patient_daily') }}
	)

	, users as (
		select * from {{ ref('scribe_users') }}
	)

	, organizations as (
		select * from {{ ref('scribe_organizations') }}
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

	, recent_nps_respondents as (
		select user_id
		from nps_survey
		where nps_survey.timestamp > current_date - interval '56 days'
		group by 1
	)

select users.user_id
	, users.email
	, posts_daily.episode_id
	, coalesce(
		lower(users.language),
		case
			when organizations.email_preference
				in ('bilingual-french-english', 'french')
			then 'fr'
			else 'en'
		end,
		'en'
	) as locale
	, current_timestamp as timestamp
	, min(posts_daily.date_day) as last_active_date
from posts_daily
left join recent_nps_respondents
	using (user_id)
inner join users
	using (user_id)
inner join episodes
	using (episode_id)
inner join organizations
	using (organization_id)
where posts_daily.date_day >= current_date - interval '7 days'
	and recent_nps_respondents.user_id is null
	and episodes.set_resolved_pending
	and episodes.outcome not in (
		'admin', 'audit', 'episode_duplicate', 'inappropriate_profile',
		'new_dependant', 'patient_unresponsive', 'test')
group by 1,2,3,4
having
	min(posts_daily.date_day) = current_date - interval '7 days'
	and max(posts_daily.date_day) = current_date - interval '7 days'
