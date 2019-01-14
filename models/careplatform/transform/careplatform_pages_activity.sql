with careplatform_pages as (
        select * from {{ ref('careplatform_pages') }}
    )

    , pages_tmp as (
        select pages.user_id
            , date_trunc('day',
                timezone('America/Montreal', pages.timestamp)
                ) as date_day_est
            , date_trunc('day', pages.timestamp) as date_day
            , timezone('America/Montreal', pages.timestamp) as timestamp_est
            , pages.timestamp
            , case
                when pages.path like '/chat/%'  then 'chat'
                when pages.path like '/consult%' then 'video'
                else 'dashboard'
              end as activity
            , case
                when pages.path like '/consult/%'
                    then split_part(pages.path, '/'::text, 4)
                when pages.path like '/chat/%' and pages.path like '%channelId%'
                    then split_part(pages.path, '='::text, 2)
                when pages.path like '/chat/%' and pages.path not like
                    '%channelId%' and split_part(pages.path, '/'::text, 4) <> ''
                    then split_part(pages.path, '/'::text, 4)
                else pages.episode_id
              end as episode_id
        from careplatform_pages as pages
        where date_trunc('day', timestamp) >= '2018-01-01'
          and pages.path is not null
    )

select user_id
    , date_day_est
    , date_day
    , timestamp_est
    , timestamp
    , activity
    , case when episode_id like '%?%' then split_part(episode_id, '?', 1)
       when episode_id like '%;%' then split_part(episode_id, ';', 1)
       else episode_id end as episode_id
from pages_tmp
where user_id is not null
    and (episode_id is not null or activity = 'dashboard')
