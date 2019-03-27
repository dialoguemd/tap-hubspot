with
	chats as (
		select * from {{ ref('chats') }}
	)

	, users as (
		select * from {{ ref('scribe_users_detailed') }}
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

	, episodes_valid as (
		select *
		from episodes
		where (
			outcome_category <> 'Unsuitable episode'
			or outcome is null
		) and first_message_patient is not null
	)

	, family_activated as (
		select family_id
			, min(first_message_patient) as activated_at
		from episodes_valid
		group by 1
	)

	, user_episodes as (
		select family_activated.family_id
			, family_activated.activated_at
			, count(distinct episodes_valid.episode_id) as episodes_year_1
		from family_activated
		inner join episodes_valid
			using (family_id)
		where episodes_valid.first_message_patient
			<= family_activated.activated_at + interval '12 months'
	)

select *
from user_episodes
