
{{
  config(
    materialized='incremental',
    unique_key='cp_activity_id',
    post_hook=[
       "{{ postgres.index(this, 'cp_activity_id')}}",
    ]
  )
}}

with careplatform_pages as (
        select * from {{ ref('careplatform_pages_activity') }}
        where timestamp < date_trunc('day', current_timestamp)
        {% if is_incremental() %}
            and timestamp > (select max(timestamp) from {{ this }})
        {% endif %}
    )

    , posts_tmp as (
        select * from {{ ref('messaging_posts_all_time')}}
        where created_at < date_trunc('day', current_timestamp)
        {% if is_incremental() %}
            and created_at > (select max(timestamp) from {{ this }})
        {% endif %}
    )

    , videos_tmp as (
        select * from {{ ref('videos_started')}}
        where timestamp < date_trunc('day', current_timestamp)
        {% if is_incremental() %}
            and timestamp > (select max(timestamp) from {{ this }})
        {% endif %}
    )

    , phone_calls_tmp as (
        select * from {{ ref('telephone_calls')}}
        where started_at < date_trunc('day', current_timestamp)
        {% if is_incremental() %}
            and started_at > (select max(timestamp) from {{ this }})
        {% endif %}
    )

    , posts as (
        select user_id
            , created_at_day_est as date_day_est
            , created_at_day as date_day
            , created_at_est as timestamp_est
            , created_at as timestamp
            , 'chat' :: text as activity
            , episode_id
        from posts_tmp
        where user_type = 'physician'
    )

    , videos as (
        select careplatform_user_id as user_id
            , date_trunc('day', timestamp_est) as date_day_est
            , date_trunc('day', timestamp) as date_day
            , timestamp_est
            , timestamp
            , 'video' :: text as activity
            , episode_id
        from videos_tmp
        -- exclude old videos that are not associated with a cp user
        where careplatform_user_id is not null
    )

    , unioned as (
        select * from careplatform_pages
        union all
        select * from posts
        union all
        select * from videos
    )

    -- Only include events that did not occur during a phone call
    , events_outside_calls as (
        select unioned.*
            -- Rank just in case there are unioned events with the same user_id
            -- and timestamp
            , row_number()
                over (partition by unioned.user_id, unioned.timestamp order by unioned.timestamp)
                as rank
        from unioned
        left join phone_calls_tmp
            on unioned.timestamp <@ phone_calls_tmp.call_range
        where phone_calls_tmp.call_id is null
    )

    , phone_calls as (
        select user_id
            , date_trunc('day', started_at) as date_day_est
            , date_trunc('day', started_at_est) as date_day
            , started_at_est as timestamp_est
            , started_at as timestamp
            , 'phone_call' :: text as activity
            , episode_id
        from phone_calls_tmp
    )

    , final as (
        select user_id
            , date_day_est
            , date_day
            , timestamp_est
            , timestamp
            , activity
            , episode_id
        from events_outside_calls
        where rank = 1
        union all
        select * from phone_calls
    )

select *
    , md5(user_id || timestamp) as cp_activity_id
from final
