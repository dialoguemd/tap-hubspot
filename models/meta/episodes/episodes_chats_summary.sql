with channels as (
        select * from {{ ref('messaging_channels') }}
    )

    , chats_all_time as (
        select * from {{ ref('chats') }}
    )

    select channels.episode_id
        , channels.user_id
        , 'https://zorro.dialogue.co/conversations/' || channels.episode_id as url_zorro
        , min(chats_all_time.first_message_created_at) as first_message_created_at
        , max(chats_all_time.last_message_created_at) as last_message_created_at
        , min(chats_all_time.first_message_care_team) as first_message_care_team
        , min(chats_all_time.first_message_nurse) as first_message_nurse
        , min(chats_all_time.first_message_shift_manager) as first_message_shift_manager
        , max(chats_all_time.last_message_care_team) as last_message_care_team
        , min(chats_all_time.first_message_patient) as first_message_patient
        , max(chats_all_time.last_message_patient) as last_message_patient
        , sum(chats_all_time.messages_total) as messages_total
        , sum(chats_all_time.messages_patient) as messages_patient
        , sum(chats_all_time.messages_care_team) as messages_care_team
        , sum(chats_all_time.messages_length_total) as messages_length_total
        , min(chats_all_time.first_set_resolved_pending_at) as first_set_resolved_pending_at
        , min(chats_all_time.first_set_active) as first_set_active
        , bool_or(chats_all_time.set_resolved_pending) as set_resolved_pending
        , bool_or(chats_all_time.chat_type = 'Follow-up') as includes_follow_up
        , bool_or(chats_all_time.includes_video) as includes_video
        , bool_or(chats_all_time.includes_video_np) as includes_video_np
        , bool_or(chats_all_time.includes_video_gp) as includes_video_gp
        , bool_or(chats_all_time.includes_video_nc) as includes_video_nc
        , bool_or(chats_all_time.includes_video_cc) as includes_video_cc
        , bool_or(chats_all_time.includes_video_psy) as includes_video_psy
    from chats_all_time
    left join channels
        using (episode_id)
    group by 1,2,3
