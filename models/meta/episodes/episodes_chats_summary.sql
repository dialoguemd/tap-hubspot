with
	channels as (
		select * from {{ ref('messaging_channels') }}
	)

	, chats as (
		select * from {{ ref('chats') }}
	)

	, chats_ranked as (
		select *
			, row_number() over(
				partition by episode_id
				order by first_message_created_at
			) as rank
		from chats
	)

	, first_chat_in_episode as (
		select *
		from chats_ranked
		where rank = 1
	)

select chats.episode_id
	, channels.user_id
	, 'https://zorro.dialogue.co/conversations/' || chats.episode_id as url_zorro
	, first_chat_in_episode.is_first_message_in_opening_hours
	, min(chats.first_message_created_at) as first_message_created_at
	, max(chats.last_message_created_at) as last_message_created_at
	, min(chats.first_message_care_team) as first_message_care_team
	, min(chats.first_message_nurse) as first_message_nurse
	, min(chats.first_message_shift_manager) as first_message_shift_manager
	, max(chats.last_message_care_team) as last_message_care_team
	, min(chats.first_message_patient) as first_message_patient
	, max(chats.last_message_patient) as last_message_patient
	, min(chats.first_message_from_last_cc) as first_message_from_last_cc
	, min(chats.first_message_from_last_nc) as first_message_from_last_nc
	, min(chats.last_message_from_last_cc) as last_message_from_last_cc
	, min(chats.last_message_from_last_nc) as last_message_from_last_nc
	, sum(chats.messages_total) as messages_total
	, sum(chats.messages_patient) as messages_patient
	, sum(chats.messages_care_team) as messages_care_team
	, sum(chats.messages_length_total) as messages_length_total
	, min(chats.first_set_resolved_pending_at) as first_set_resolved_pending_at
	, min(chats.first_set_active) as first_set_active
	, bool_or(chats.set_resolved_pending) as set_resolved_pending
	, bool_or(chats.chat_type = 'Follow-up') as includes_follow_up
	, bool_or(chats.includes_video) as includes_video
	, bool_or(chats.includes_video_np) as includes_video_np
	, bool_or(chats.includes_video_gp) as includes_video_gp
	, bool_or(chats.includes_video_nc) as includes_video_nc
	, bool_or(chats.includes_video_cc) as includes_video_cc
	, bool_or(chats.includes_video_psy) as includes_video_psy
from chats
left join channels
	using (episode_id)
left join first_chat_in_episode
	using (episode_id)
{{ dbt_utils.group_by(n=4) }}
