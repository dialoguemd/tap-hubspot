with
	chats as (
		select * from {{ ref('chats') }}
	)

	, episodes_outcomes as (
		select * from {{ ref('episodes_outcomes') }}
	)

	, chats_ooh as (
		select chats.date_day_est
			, chats.user_id
			, chats.wait_time_first_care_team
			, chats.first_message_care_team
			, chats.episode_id
			, extract(hour from chats.first_message_patient) > '12'
				as is_pm_chat
			, chats.opening_hour_est
		from chats
		left join episodes_outcomes
			using (episode_id)
		where not chats.is_first_message_in_opening_hours
			and chats.chat_type = 'New Episode'
			and chats.initiator = 'patient'
			and (episodes_outcomes.outcome
				not in ('test', 'episode_duplicate', 'test', 'admin')
				or episodes_outcomes.outcome is null)
	)

	-- Chats started after closing hours
	-- Look at the first response in the same day/chat or the
	-- first response on the next day
	, chats_ooh_pm as (
		select chats_first.date_day_est
			, chats_first.user_id
			, chats_first.episode_id
			, chats_first.wait_time_first_care_team is not null
				or chats_next_day.first_message_care_team is not null
				as is_answered_next_day
			, case
				-- message answered on the same day
				when chats_first.wait_time_first_care_team is not null
				then true
				-- not answered on the next day
				-- or next day is closed
				when chats_next_day.first_message_care_team is null
					or chats_next_day.opening_hour_est is null
				then false
				else extract(epoch from
					chats_next_day.first_message_care_team
					- chats_next_day.opening_hour_est
				) < 10800
			end as is_answered_within_3_opened_hours
			, is_pm_chat
		from chats_ooh as chats_first
		left join chats as chats_next_day
			on chats_first.episode_id = chats_next_day.episode_id
			and chats_first.date_day_est + interval '1 day'
				= chats_next_day.date_day_est
		where chats_first.is_pm_chat
	)

	-- Chats started before opening hours
	-- Look at the first response in the same day/chat
	, chats_ooh_am as (
		select date_day_est
			, user_id
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
		from chats_ooh
		where not is_pm_chat
	)

select * from chats_ooh_pm
union all
select * from chats_ooh_am
