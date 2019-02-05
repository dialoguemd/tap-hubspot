with
	messaging as (
		select * from {{ ref('chats_messaging_daily') }}
	)

	, state_changes as (
		select * from {{ ref('chats_state_changes_daily') }}
	)

	, reminders as (
		select * from {{ ref('chats_reminders_daily') }}
	)

	, outcomes as (
		select * from {{ ref('chats_outcomes_daily') }}
	)

	, videos as (
		select * from {{ ref('videos_by_episode_daily') }}
	)

select messaging.date_day_est
	, messaging.date_week_est as date_week
	, messaging.date_week_est
	, messaging.episode_id
	, messaging.first_message_care_team
	, messaging.first_message_nurse
	, messaging.first_message_shift_manager
	, messaging.last_message_care_team
	, messaging.first_message_patient
	, messaging.last_message_patient
	, messaging.end_first_patient_sequence
	, messaging.first_care_team_user_id
	, messaging.first_care_team_user_name
	, messaging.messages_total
	, messaging.messages_patient
	, messaging.messages_care_team
	, messaging.messages_length_total
	, messaging.first_message_created_at
	, messaging.last_message_created_at
	, messaging.user_id
	, messaging.includes_care_team_message
	, messaging.includes_nurse_message
	, messaging.includes_sm_message
	, messaging.includes_patient_message
	, messaging.timespan
	, messaging.initiator
	, messaging.wait_time_first_care_team
	, messaging.wait_time_first_nurse
	, messaging.wait_time_first_shift_manager
	, messaging.wait_time_first_sequence_care_team
	, messaging.wait_time_first_sequence_nurse
	, messaging.wait_time_first_sequence_shift_manager
	, messaging.rank_chat_in_episode
	, messaging.url_zorro
	, messaging.cp_deep_link
	, messaging.time_since_last_message
	, messaging.avg_wait_time_following_messages
	, messaging.is_first_message_in_opening_hours
	, messaging.opening_hour_est
	, messaging.closing_hour_est

	, coalesce(reminders.has_open_reminder, false) as has_open_reminder
	, coalesce(state_changes.set_resolved_pending, false) as set_resolved_pending
	, state_changes.first_set_resolved_pending_at
	, state_changes.first_set_active
	, case
		when state_changes.set_resolved_pending
			then extract('epoch'
			from state_changes.first_set_resolved_pending_at
				- messaging.first_message_created_at) / 60.0
			else null
			end as time_to_resolved_pending

	, outcomes.valid_outcomes
	, outcomes.invalid_outcomes

	, videos.episode_id is not null as includes_video
	, coalesce(videos.includes_video_gp, false) as includes_video_gp
	, coalesce(videos.includes_video_np, false) as includes_video_np
	, coalesce(videos.includes_video_nc, false) as includes_video_nc
	, coalesce(videos.includes_video_cc, false) as includes_video_cc
	, coalesce(videos.includes_video_psy, false) as includes_video_psy
	, videos.video_start_time_gp_np

	, case
		when messaging.rank_chat_in_episode = 1 then 'New Episode'
		when messaging.initiator = 'care-team'
			and reminders.has_open_reminder then 'Follow-up'
		when videos.episode_id is not null then 'Video'
		when messaging.initiator = 'care-team' then 'Other initiated by Care Team'
		when messaging.initiator = 'patient' then 'Other initiated by patient'
		else 'unknown'
		end as chat_type
from messaging
left join state_changes
	using (episode_id, date_day_est)
left join reminders
	using (episode_id, date_day_est)
left join outcomes
	using (episode_id, date_day_est)
left join videos
	using (episode_id, date_day_est)
