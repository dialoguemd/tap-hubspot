with
    qnaire as (
        select * from {{ ref('countdown_qnaire_completion_stats') }}
    )

    , posts as (
        select * from {{ ref('messaging_posts_all_time') }}
    )
    
    , event_name_mapping as (
        select * from {{ ref('intake_event_name_mapping') }}
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
            , qnaire as event_name
            , 'qnaire' as type
            , 'system' as initiator
            , user_id
            , episode_id
        from qnaire
        where started_at <@
            tstzrange(current_date - interval '4 weeks',
                current_date - interval '1 day')
            and qnaire not in ('paused', 'location_serviceability', 'with_you_shortly')

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
    )

    , events_lagged as (
        select *
            , lag(event_name) over (partition by episode_id order by timestamp)
        from events_all
    )
    
    , events_contextualized as (
        select timestamp
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

select events_contextualized.timestamp
    , events_contextualized.event_name
    , event_name_mapping.event_grouping
    , events_contextualized.user_id
    , events_contextualized.episode_id
    , events_contextualized.initiator
    , events_contextualized.type
    , row_number() over (partition by events_contextualized.episode_id
        order by events_contextualized.timestamp) as rank
    , lead(events_contextualized.type) over 
        (partition by events_contextualized.episode_id
            order by events_contextualized.timestamp)
        as following_type
    , lead(events_contextualized.event_name) over
        (partition by events_contextualized.episode_id
            order by events_contextualized.timestamp)
        as following_event
    -- NB: duration could be modified with an is_active like condition to get
    -- more accurate duration estimates for how long these events take 
    , extract(epoch from
        lead(events_contextualized.timestamp) over
            (partition by events_contextualized.episode_id
                order by events_contextualized.timestamp)
        - events_contextualized.timestamp) as duration
from events_contextualized
left join event_name_mapping
    using (event_name)
