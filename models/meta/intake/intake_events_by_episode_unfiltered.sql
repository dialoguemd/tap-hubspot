with
    qnaire as (
        select * from {{ ref('countdown_qnaire_completion_stats') }}
    )

    , posts as (
        select * from {{ ref('messaging_posts_all_time') }}
    )

    -- Accuracy of this tracking is suspect so it's been commented out for now
    -- , apt_booking as (
    --     select * from {{ ref('careplatform_appointment_booking_started') }}
    -- )

    , outcome_set as (
        select * from {{ ref('careplatform_outcome_set') }}
    )

    , posts_excl_responses as (
        select posts.created_at
            , posts.user_id
            , posts.episode_id 
            , posts.user_type
        from posts
        -- join to exclude posts that occurred during a qnaire
        left join qnaire
            on posts.episode_id = qnaire.episode_id
            and tstzrange(qnaire.started_at, qnaire.completed_at)
                @> posts.created_at
            and qnaire.questionnaire_completion_time > 0
        where posts.is_internal_post is false 
            -- Exclude the system user
            and posts.user_id <> 'zy4q8gkk7bn67f6q7345qwztyo'
            and qnaire.episode_id is null
    )

    , events_all as (

        -- Filter timestamps in this union rather than downstream to optimize
        -- query run time

        select started_at as timestamp
            , qnaire_name as event_name
            , 'qnaire' as type
            , 'system' as initiator
            , user_id
            , episode_id
        from qnaire
        where started_at <@
            tstzrange(current_date - interval '4 weeks',
                current_date - interval '1 day')
            and qnaire_name not in ('paused', 'location_serviceability', 'with_you_shortly')

        union all

        select created_at as timestamp
            , 'free_text' as event_name
            , 'free_text' as type
            , user_type as initiator
            , user_id
            , episode_id
        from posts_excl_responses
        where created_at <@
            tstzrange(current_date - interval '4 weeks',
                current_date - interval '1 day')

        union all

        -- select timestamp
        --     , 'appointment_booking' as event_name
        --     , 'appointment_booking' as type
        --     , 'practitioner' as initiator
        --     , user_id
        --     , episode_id
        -- from apt_booking
        -- where timestamp <@
        --     tstzrange(current_date - interval '4 weeks',
        --         current_date - interval '1 day')

        -- union all

        select timestamp
            , outcome as event_name
            , 'resolved' as type
            , 'practitioner' as initiator
            , user_id
            , episode_id
        from outcome_set
        where timestamp <@
            tstzrange(current_date - interval '4 weeks',
                current_date - interval '1 day')

    )

    , events_lagged as (
        select *
            , lag(event_name) over (partition by episode_id order by timestamp)
        from events_all
    )
    
    , events_contextualized as (
        select timezone('America/Montreal', timestamp) as timestamp_est
            , timestamp
            , user_id
            , episode_id
            , type
            , initiator
            , case when event_name = 'free_text'
                then 'free_text_following_' || lag
                else event_name end as event_name
        from events_lagged
        -- Exclude events where both the event name and the lagged event name
        -- are 'free_text'; this logic will transform a series of sequential
        -- free_text events into one free_text event (with context)
        where event_name <> 'free_text' or lag <> 'free_text'
    )

select timestamp
    , timestamp_est
    , tsrange(timestamp_est,
        lead(timestamp_est) over
            (partition by episode_id
                order by timestamp_est))
            as during
    , event_name
    , case when event_name = 'dxa' then 'dxa' else type end as event_grouping
    , user_id
    , episode_id
    , initiator
    , type
    , row_number() over (partition by episode_id
        order by timestamp) as rank
    , lead(type) over
        (partition by episode_id
            order by timestamp)
        as following_type
    , lead(event_name) over
        (partition by episode_id
            order by timestamp)
        as following_event
    -- NB: duration could be modified with an is_active like condition to get
    -- more accurate duration estimates for how long these events take 
    , extract(epoch from
        lead(timestamp) over
            (partition by episode_id
                order by timestamp)
        - timestamp) as duration
from events_contextualized
