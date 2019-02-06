-- TODO: move these properties to the episode level
with
	chats as (
		select * from {{ ref('chats') }}
	)

	, episodes_outcomes as (
		select * from {{ ref('episodes_outcomes') }}
	)

	, episodes_chief_complaint as (
		select * from {{ ref('episodes_chief_complaint') }}
	)

	, episode_set_waiting_for_patient as (
		select * from {{ ref('usher_episode_set_waiting_for_patient')}}
	)

	, set_waiting_for_patient_daily as (
		select date_day_est
			, episode_id
			, min(timestamp_est) as first_timestamp_est
		from episode_set_waiting_for_patient
		group by 1,2
	)

	, chats_ooh as (
		select chats.date_day_est
			, chats.patient_id
			,  extract('epoch' from
				least(
					episodes_chief_complaint.timestamp_est
					, chats.first_message_care_team
				)
				- chats.first_message_patient
			) / 60.0 as wait_time_first_care_team
			, least(
					episodes_chief_complaint.timestamp_est
					, chats.first_message_care_team
				) as first_message_care_team
			, chats.episode_id
			, (extract(hour from chats.first_message_patient) > '12')::boolean
				as is_pm_chat
			, chats.opening_hour_est
			, chats.first_message_patient
		from chats
		left join episodes_outcomes
			using (episode_id)
		left join episodes_chief_complaint
			using (episode_id)
		where not chats.is_first_message_in_opening_hours
			and chats.chat_type = 'New Episode'
			and chats.initiator = 'patient'
			and (episodes_outcomes.outcome
				not in ('test', 'episode_duplicate', 'admin')
				or episodes_outcomes.outcome is null)
	)

	-- Chats started after closing hours
	-- Look at the first response in the same day/chat or the
	-- first response on the next day
	, chats_ooh_pm as (
		select chats_first.date_day_est
			, chats_first.patient_id
			, chats_first.episode_id
			, (
				chats_first.wait_time_first_care_team is not null
				or chats_next_day.first_message_care_team is not null
				or set_waiting_for_patient_daily.first_timestamp_est is not null
			)::boolean as is_answered_next_day
			, (case
				-- message answered on the same day
				when chats_first.wait_time_first_care_team is not null
				then true
				-- not answered on the next day
				-- or next day is closed
				when (chats_next_day.first_message_care_team is null
					and set_waiting_for_patient_daily.first_timestamp_est is null)
					or chats_next_day.opening_hour_est is null
				then false
				else extract(epoch from
					least(
						chats_next_day.first_message_care_team
						, set_waiting_for_patient_daily.first_timestamp_est
					)
					- chats_next_day.opening_hour_est
				) < 10800
			end)::boolean as is_answered_within_3_opened_hours
			, is_pm_chat
			, chats_first.first_message_patient
		from chats_ooh as chats_first
		left join chats as chats_next_day
			on chats_first.episode_id = chats_next_day.episode_id
			and chats_first.date_day_est + interval '1 day'
				= chats_next_day.date_day_est
		left join set_waiting_for_patient_daily
			on chats_first.episode_id
				= set_waiting_for_patient_daily.episode_id
			and chats_first.date_day_est + interval '1 day'
				= set_waiting_for_patient_daily.date_day_est
		where chats_first.is_pm_chat
	)

	-- Chats started before opening hours
	-- Look at the first response in the same day/chat
	, chats_ooh_am as (
		select date_day_est
			, patient_id
			, episode_id
			, wait_time_first_care_team is not null as is_answered_next_day
			, case
				when first_message_care_team is null
					or opening_hour_est is null
				then false
				else
					extract(epoch from
					first_message_care_team - opening_hour_est
				) < 10800
			end as is_answered_within_3_opened_hours
			, is_pm_chat
			, chats_ooh.first_message_patient
		from chats_ooh
		where not is_pm_chat
	)

select * from chats_ooh_pm
union all
select * from chats_ooh_am
