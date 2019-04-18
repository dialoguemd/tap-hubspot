
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

    -- Identify all events that occurred during a phone call to exclude them later
    , overlap as (
        select unioned.*
        from unioned
        left join phone_calls_tmp
            on unioned.timestamp <@ phone_calls_tmp.call_range
        where phone_calls_tmp.call_id is not null
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
        select * from overlap
        union all
        select * from phone_calls
    )

select *
    , md5(user_id || timestamp) as cp_activity_id
from final
