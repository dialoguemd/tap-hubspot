with messaging_posts_all_time as (
        select * from {{ ref('messaging_posts_all_time') }}
    )

    , careplatform_reminders_status_updated as (
        select * from {{ ref('careplatform_reminders_status_updated') }}
    )

    , careplatform_reminder_created as (
        select * from {{ ref('careplatform_reminder_created') }}
    )

    , usher_episode_state_updated as (
        select * from {{ ref('usher_episode_state_updated') }}
    )

    , unresponsive_snooze_workflow_finished as (
        select * from {{ ref('unresponsive_snooze_workflow_finished') }}
    )

    , careplatform_video_stream_created as (
        select * from {{ ref('careplatform_video_stream_created') }}
    )

    , wiw_opening_hours as (
        select * from {{ ref('wiw_opening_hours') }}
    )

    , posts as (
        select posts.created_at at time zone 'America/Montreal' as created_at
          , date_trunc('day', posts.created_at at time zone 'America/Montreal') as created_at_day
          , posts.user_id
          , posts.channel_id
          , posts.message_length
          , coalesce(users.is_care_team, false) as is_care_team
          , row_number()
              over (
                partition by posts.channel_id
                  , date_trunc('day', posts.created_at at time zone 'America/Montreal')
                  , coalesce(users.is_care_team, false)
                order by posts.created_at
              ) as rank_user
          , extract('epoch' from
            (posts.created_at
            - lag(posts.created_at)
              over (
                partition by posts.channel_id
                  , date_trunc('day', posts.created_at at time zone 'America/Montreal')
                order by posts.created_at
              ))) / 60.0 as time_since_last_message
          , lag(coalesce(users.is_care_team, false))
              over (
                partition by posts.channel_id
                  , date_trunc('day', posts.created_at at time zone 'America/Montreal')
                order by posts.created_at
              ) as is_last_message_care_team
          , users.first_name || ' ' || users.last_name as user_name
          , users.main_specialization
          , row_number() over (
              partition by posts.channel_id
              order by posts.created_at
            ) as rank_chat_in_episode
        from messaging_posts_all_time as posts
        inner join pdt.users
          on posts.user_id = users.user_id
        where not posts.is_internal_post
    )

    , chats as (
        select
            date_trunc('day', created_at) as created_at_day
            , channel_id
            , min(created_at) filter(where is_care_team) as first_message_care_team
            , min(created_at) filter(where
                main_specialization in ('Nurse Clinician', 'Nurse Practitioner')
              ) as first_message_nurse
            , max(created_at) filter(where is_care_team) as last_message_care_team
            , min(created_at) filter(where not is_care_team) as first_message_patient
            , max(created_at) filter(where not is_care_team) as last_message_patient
            , count(*) as messages_total
            , count(*) filter(where not is_care_team) as messages_patient
            , count(*) filter(where is_care_team) as messages_care_team
            , sum(message_length) as messages_length_total
            , min(created_at) as first_message_created_at
            , max(created_at) as last_message_created_at
            , max(user_id) filter(where not is_care_team) as user_id
            , max(user_id) filter(where rank_user=1 and is_care_team)
                as first_care_team_user_id
            , max(user_name) filter(where rank_user=1 and is_care_team)
                as first_care_team_user_name
            -- time between first nurse message and last user message (in his first burst)
            , max(time_since_last_message) filter(where
                rank_user = 1 and is_care_team) as time_since_last_message
            , avg(time_since_last_message) filter(where
                rank_user > 1 and rank_user < 7
                and not is_last_message_care_team and is_care_team
              ) as avg_wait_time_following_messages
            , min(rank_chat_in_episode) as rank_chat_in_episode
        from posts
        group by 1,2
    )

    , first_patient_message_sequence as (
        select posts.created_at_day
          , posts.channel_id
          , max(posts.created_at) as end_of_first_patient_message_sequence
        from posts
        inner join chats
          on posts.channel_id = chats.channel_id
            and posts.created_at_day = chats.created_at_day
            and posts.created_at < chats.first_message_care_team
        where not posts.is_care_team
        group by 1,2
    )

    , reminders_completed as (
        select reminder_id
          , min(updated_at) as completed_at
        from careplatform_reminders_status_updated
        where reminder_status = 'completed'
        group by 1
    )

    , reminders as (
        select careplatform_reminder_created.reminder_id
          , careplatform_reminder_created.episode_id as episode_id
          , timezone('America/Montreal', careplatform_reminder_created.due_at) as due_at
          , timezone('America/Montreal', reminders_completed.completed_at) as completed_at
          , tsrange(
              date_trunc('day', timezone('America/Montreal', careplatform_reminder_created.due_at)),
              timezone('America/Montreal', reminders_completed.completed_at)
            ) as reminder_opened_range
        from careplatform_reminder_created
        left join reminders_completed
          on careplatform_reminder_created.reminder_id = reminders_completed.reminder_id
        where -- exclude same day reminders
          date_trunc('day', timezone('America/Montreal', careplatform_reminder_created.created_at))
            <> date_trunc('day', timezone('America/Montreal', careplatform_reminder_created.due_at))
          and date_trunc('day', timezone('America/Montreal', careplatform_reminder_created.due_at))
            <= date_trunc('day', timezone('America/Montreal', reminders_completed.completed_at))
    )

    , set_episode_state as (
        select episode_id
          , updated_at
          , episode_state
        from usher_episode_state_updated
        union all
        select episode_id
          , workflow_finished_at as updated_at
          , 'resolved' as episode_state
        from unresponsive_snooze_workflow_finished
    )

    , episode_pending_resolved as (
        select episode_id as channel_id
          , date_trunc('day', timezone('America/Montreal', updated_at)) as created_at_day
          , min(timezone('America/Montreal', updated_at)) as first_set_resolved_pending_at
        from set_episode_state
        inner join chats
        on set_episode_state.episode_id = chats.channel_id
          and date_trunc('day', timezone('America/Montreal', set_episode_state.updated_at)) = chats.created_at_day
        where set_episode_state.episode_state in ('resolved', 'pending')
        and timezone('America/Montreal', set_episode_state.updated_at) >= chats.first_message_created_at
        group by 1,2
    )

    , videos as (
        select date_trunc('day', timezone('America/Montreal', careplatform_video_stream_created.created_at)) as sent_at_day
          , careplatform_video_stream_created.episode_id
          , min(timezone('America/Montreal', careplatform_video_stream_created.created_at)) as first_video_gp_np_started_at
        from careplatform_video_stream_created
        inner join pdt.users as care_team_user
          on careplatform_video_stream_created.practitioner_id = care_team_user.user_id
            and care_team_user.is_care_team
            and care_team_user.main_specialization
              in ('Family Physician', 'Nurse Practitioner')
        group by 1,2
    )

    , chats_full as (
        select chats.created_at_day
        , chats.channel_id
        , 'https://zorro.dialogue.co/conversations/'
            || chats.channel_id as url_zorro
        , 'careplatform://chat/' || chats.user_id || '/' || chats.channel_id as cp_deep_link
        , chats.first_message_care_team
        , chats.first_message_nurse
        , chats.last_message_care_team
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
        , chats.first_message_care_team is not null as includes_care_team_message
        , chats.first_message_nurse is not null as includes_nurse_message
        , chats.first_message_patient is not null as includes_patient_message
        , case
            when chats.first_message_patient is not null
              and chats.first_message_care_team is not null
            then extract('epoch'
              from chats.last_message_created_at - chats.first_message_created_at) / 60.0
            else null
          end as timespan
        , case
            when chats.first_message_patient is null then 'care-team'
            when chats.first_message_care_team is null then 'patient'
            when chats.first_message_patient < chats.first_message_care_team
            then 'patient'
            else 'care-team'
          end as initiator
        , case
            when chats.first_message_patient < chats.first_message_care_team
            then extract('epoch' from chats.first_message_care_team - chats.first_message_patient)
              / 60.0
            else null
          end as wait_time_first
        , case
            when chats.first_message_patient < chats.first_message_nurse
            then extract('epoch' from chats.first_message_nurse - chats.first_message_patient)
              / 60.0
            else null
          end as wait_time_first_nurse
        , case
            when chats.first_message_patient < chats.first_message_care_team
            then extract('epoch'
                from chats.first_message_care_team
                - first_patient_message_sequence.end_of_first_patient_message_sequence
              ) / 60.0
            else null
          end as wait_time_end_of_first_message_sequence
        , case
            when chats.first_message_patient < chats.first_message_care_team
            then extract('epoch'
                from chats.first_message_nurse
                - first_patient_message_sequence.end_of_first_patient_message_sequence
              ) / 60.0
            else null
          end as wait_time_end_of_first_message_sequence_nurse
        , case
            when chats.first_message_patient < chats.first_message_care_team
            then chats.time_since_last_message
            else null
          end as time_since_last_message
        , chats.avg_wait_time_following_messages
        , wiw_opening_hours.date is not null as is_first_message_in_opening_hours
        , reminders.episode_id is not null as has_open_reminder
        , episode_pending_resolved.first_set_resolved_pending_at is not null
            as set_resolved_pending
        , episode_pending_resolved.first_set_resolved_pending_at as first_set_resolved_pending_at
        , first_patient_message_sequence.end_of_first_patient_message_sequence
        , chats.rank_chat_in_episode
        from chats
        left join wiw_opening_hours
        on chats.first_message_created_at <@ wiw_opening_hours.opening_span_est
        left join reminders
        on chats.channel_id = reminders.episode_id
          and chats.first_message_created_at <@ reminders.reminder_opened_range
        left join episode_pending_resolved
        on chats.channel_id = episode_pending_resolved.channel_id
          and chats.created_at_day = episode_pending_resolved.created_at_day
        left join first_patient_message_sequence
        on chats.channel_id = first_patient_message_sequence.channel_id
          and chats.created_at_day = first_patient_message_sequence.created_at_day
    )

    select chats_full.created_at_day
        , chats_full.channel_id
        , chats_full.first_message_care_team
        , chats_full.first_message_nurse
        , chats_full.last_message_care_team
        , chats_full.first_message_patient
        , chats_full.last_message_patient
        , chats_full.end_of_first_patient_message_sequence
        , chats_full.first_care_team_user_id
        , chats_full.first_care_team_user_name
        , chats_full.messages_total
        , chats_full.messages_patient
        , chats_full.messages_care_team
        , chats_full.messages_length_total
        , chats_full.first_message_created_at
        , chats_full.last_message_created_at
        , chats_full.user_id
        , chats_full.includes_care_team_message
        , chats_full.includes_nurse_message
        , chats_full.includes_patient_message
        , chats_full.timespan
        , chats_full.initiator
        , chats_full.wait_time_first
        , chats_full.wait_time_first_nurse
        , chats_full.wait_time_end_of_first_message_sequence
        , chats_full.wait_time_end_of_first_message_sequence_nurse
        , chats_full.rank_chat_in_episode
        , chats_full.url_zorro
        , chats_full.cp_deep_link
        , chats_full.time_since_last_message
        , chats_full.avg_wait_time_following_messages
        , chats_full.is_first_message_in_opening_hours
        , chats_full.has_open_reminder
        , chats_full.set_resolved_pending
        , chats_full.first_set_resolved_pending_at
        , case
          when chats_full.set_resolved_pending
          then extract('epoch'
            from chats_full.first_set_resolved_pending_at - chats_full.first_message_created_at) / 60.0
          else null
        end as time_to_resolved_pending
        , chats_full.initiator = 'care-team' and chats_full.has_open_reminder as is_follow_up
        , rank_chat_in_episode = 1 as first_chat_in_episode
        , videos.episode_id is not null as includes_video
        , videos.first_video_gp_np_started_at as video_start_time
        , case
            when rank_chat_in_episode = 1 then 'New Episode'
            when initiator = 'care-team' and has_open_reminder then 'Follow-up'
            when videos.episode_id is not null then 'Video'
            when initiator = 'care-team' then 'Other initiated by Care Team'
            when initiator = 'patient' then 'Other initiated by patient'
            else 'unknown'
          end as chat_type
    from chats_full
    left join videos
    on chats_full.channel_id = videos.episode_id
      and chats_full.created_at_day = videos.sent_at_day