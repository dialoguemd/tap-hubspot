with careplatform_pages as (
        select * from {{ ref('careplatform_pages_activity') }}
    )

    , posts_tmp as (
        select * from {{ ref('messaging_posts_all_time')}}
    )

    , videos_tmp as (
        select * from {{ ref('videos_started')}}
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
            , date_trunc('day', started_at_est) as date_day_est
            , date_trunc('day', started_at) as date_day
            , started_at_est as timestamp_est
            , started_at as timestamp
            , 'video' :: text as activity
            , episode_id
        from videos_tmp
    )

    , unioned as (
        select * from careplatform_pages
        union all
        select * from posts
        union all
        select * from videos
    )

select * from unioned
