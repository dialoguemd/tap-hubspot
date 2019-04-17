
{{
  config(
    materialized='incremental',
    unique_key='chat_id',
    post_hook=[
       "{{ postgres.index(this, 'chat_id')}}",
    ]
  )
}}

with
	posts_all_time as (
		select * from {{ ref('messaging_posts_all_time') }}
		-- Do not pull data for today because these chats' facts may change
		-- during the day (day defined as EST)
		where created_at_day_est
			< date_trunc('day', timezone('America/Montreal', current_timestamp))
		-- Implement as incremental
		{% if is_incremental() %}
			and created_at_day_est > (select max(date_day_est) from {{ this }})
		{% endif %}
	)

	, wiw_shifts as (
		select * from {{ ref('wiw_shifts') }}
	)

	, practitioners as (
		select * from {{ ref('practitioners') }}
	)

	, wiw_opening_hours as (
		select * from {{ ref('wiw_opening_hours') }}
	)

	, posts as (
		select posts_all_time.created_at_est
			, posts_all_time.created_at_day_est
			, posts_all_time.user_id
			, posts_all_time.user_type
			, posts_all_time.episode_id
			, posts_all_time.message_length
			, practitioners.user_name
			, practitioners.user_id as practitioner_id
			, practitioners.main_specialization
			, last_value(practitioners.user_id) over
				(partition by posts_all_time.created_at_day_est,
						posts_all_time.episode_id,
						practitioners.main_specialization
					order by posts_all_time.created_at_est
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
				as last_user_id_by_specialization
			, wiw_shifts.position_name
			, practitioners.user_id is not null as is_care_team
			, row_number()
				over (
					partition by posts_all_time.episode_id
					, posts_all_time.created_at_day_est
					, practitioners.user_id is not null
					order by posts_all_time.created_at_est
				) as rank_user
			, extract('epoch' from
				(posts_all_time.created_at_est
				- lag(posts_all_time.created_at_est) over (
					partition by posts_all_time.episode_id
					, posts_all_time.created_at_day_est
					order by posts_all_time.created_at_est
				))) / 60.0 as time_since_last_message
			, lag(practitioners.user_id is not null)
				over (
					partition by posts_all_time.episode_id
					, posts_all_time.created_at_day_est
					order by posts_all_time.created_at_est
				) as is_last_message_care_team
		from posts_all_time
		left join practitioners
			using (user_id)
		left join wiw_shifts
			on posts_all_time.user_id = wiw_shifts.user_id
			and posts_all_time.created_at <@ wiw_shifts.shift_schedule
		where not posts_all_time.is_internal_post
	)

	, chats as (
		select
			created_at_day_est
			, episode_id

			, min(created_at_est) as first_message_created_at
			, max(created_at_est) as last_message_created_at

			, min(created_at_est)
				filter(where is_care_team) as first_message_care_team
			, max(created_at_est)
				filter(where is_care_team) as last_message_care_team

			, min(created_at_est)
				filter(where main_specialization in
					('Nurse Clinician', 'Nurse Practitioner')
				) as first_message_nurse
			, min(created_at_est)
				filter(where main_specialization in
					('Nurse Clinician')
				) as first_message_nc
			, min(created_at_est)
				filter(where main_specialization in
					('Nurse Practitioner')
				) as first_message_np
			, min(created_at_est)
				filter(where main_specialization = 'Care Coordinator'
				and position_name = 'Shift Manager'
				) as first_message_shift_manager

			, min(created_at_est)
				filter(where main_specialization = 'Care Coordinator'
					and last_user_id_by_specialization = practitioner_id
				) as first_message_from_last_cc

			, min(created_at_est)
				filter(where main_specialization = 'Nurse Clinician'
					and last_user_id_by_specialization = practitioner_id
				) as first_message_from_last_nc

			, max(created_at_est)
				filter(where main_specialization = 'Care Coordinator'
					and last_user_id_by_specialization = practitioner_id
				) as last_message_from_last_cc

			, max(created_at_est)
				filter(where main_specialization = 'Nurse Clinician'
					and last_user_id_by_specialization = practitioner_id
				) as last_message_from_last_nc

			, min(created_at_est)
				filter(where user_type = 'patient') as first_message_patient
			, max(created_at_est)
				filter(where user_type = 'patient') as last_message_patient

			, count(*) as messages_total
			, count(*)
				filter(where user_type = 'patient') as messages_patient
			, count(*)
				filter(where is_care_team) as messages_care_team

			, sum(message_length) as messages_length_total

			, max(user_id)
				filter(where user_type = 'patient') as user_id
			, max(user_id)
				filter(where rank_user=1 and is_care_team)
				as first_care_team_user_id
			, max(user_name)
				filter(where rank_user=1) as first_care_team_user_name

			-- time between first nurse message and last user message (in his first burst)
			, max(time_since_last_message)
				filter(where rank_user = 1 and is_care_team)
				as time_since_last_message
			, avg(time_since_last_message)
				filter(where rank_user > 1 and rank_user < 7
				and not is_last_message_care_team and is_care_team
				) as avg_wait_time_following_messages

		from posts
		group by 1,2
		having min(created_at_est) filter(where user_type = 'patient') is not null
			or min(created_at_est) filter(where is_care_team) is not null
	)

	, first_patient_sequence as (
		select posts.created_at_day_est
			, posts.episode_id
			, max(posts.created_at_est) as end_first_patient_sequence
		from posts
		inner join chats
			on posts.episode_id = chats.episode_id
			and posts.created_at_day_est = chats.created_at_day_est
			and posts.created_at_est < chats.first_message_care_team
		where posts.user_type = 'patient'
		group by 1,2
	)

select chats.episode_id || chats.created_at_day_est::date as chat_id
	, chats.created_at_day_est as date_day_est
	, date_trunc('week', chats.created_at_day_est) as date_week_est
	, date_trunc('month', chats.created_at_day_est) as date_month_est
	, chats.episode_id
	, 'https://zorro.dialogue.co/conversations/' || chats.episode_id
		as url_zorro
	, 'careplatform://chat/' || chats.user_id || '/' || chats.episode_id
		as cp_deep_link
	, chats.first_message_care_team
	, chats.first_message_nurse
	, chats.first_message_shift_manager
	, chats.last_message_care_team
	, chats.first_message_from_last_cc
	, chats.first_message_from_last_nc
	, chats.last_message_from_last_cc
	, chats.last_message_from_last_nc
	, chats.first_message_patient
	, chats.last_message_patient
	, chats.messages_total
	, chats.messages_patient
	, chats.messages_care_team
	, chats.messages_length_total
	, chats.first_message_created_at
	, chats.last_message_created_at
	, chats.first_care_team_user_id
	, chats.first_care_team_user_name
	, chats.user_id
	, chats.avg_wait_time_following_messages
	, first_patient_sequence.end_first_patient_sequence

	, chats.first_message_care_team is not null as includes_care_team_message
	, chats.first_message_nurse is not null as includes_nurse_message
	, chats.first_message_shift_manager is not null as includes_sm_message
	, chats.first_message_patient is not null as includes_patient_message

	, case
		when chats.first_message_patient is not null
			and chats.first_message_care_team is not null
		then extract('epoch' from chats.last_message_created_at
			- chats.first_message_created_at) / 60.0
		else null
		end as timespan
	, case
		when chats.first_message_patient is null then 'care-team'
		when chats.first_message_care_team is null then 'patient'
		when chats.first_message_patient < chats.first_message_care_team
		then 'patient'
		else 'care-team'
		end as initiator

	-- Jinja loops for keeping window functions clean

	-- Standard wait time
	{% for type in ["care_team", "nurse", "shift_manager"] %}

	, case
		when chats.first_message_patient < chats.first_message_{{type}}
		then extract('epoch' from chats.first_message_{{type}}
			- chats.first_message_patient)/ 60.0
		else null
		end as wait_time_first_{{type}}

	{% endfor %}

	-- Wait time based on end of first patient sequence
	{% for type in ["care_team", "nurse", "shift_manager"] %}

	, case
		when chats.first_message_patient < chats.first_message_{{type}}
		then extract('epoch' from chats.first_message_{{type}}
			- first_patient_sequence.end_first_patient_sequence) / 60.0
		else null
		end as wait_time_first_sequence_{{type}}

	{% endfor %}

	, case
		when chats.first_message_patient < chats.first_message_care_team
		then chats.time_since_last_message
		else null
		end as time_since_last_message
	, chats.first_message_created_at <@ wiw_opening_hours.opening_span_est
		as is_first_message_in_opening_hours
	, wiw_opening_hours.opening_hour_est
	, wiw_opening_hours.closing_hour_est
from chats
left join wiw_opening_hours
	on chats.created_at_day_est = wiw_opening_hours.date_day
left join first_patient_sequence
	using (episode_id, created_at_day_est)
