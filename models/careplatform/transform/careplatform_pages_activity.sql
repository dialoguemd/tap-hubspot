with careplatform_pages as (
        select * from {{ ref('careplatform_pages') }}
    )

    , practitioners as (
        select * from {{ ref('coredata_practitioners')}}
    )

select pages.user_id as careplatform_user_id
    , date_trunc('day',
        timezone('America/Montreal', pages.timestamp)
        ) as date_day_est
    , date_trunc('day', pages.timestamp) as date_day
    , timezone('America/Montreal', pages.timestamp) as timestamp_est
    , pages.timestamp
    , case
        when pages.path in ('/',
                      '/pending',
                      '/resolved',
                      '/care-plans',
                      '/reminders',
                      '/mentions',
                      '/login',
                      '/reminders/completed',
                      '/snoozed') 
            then 'dashboard'
        when pages.path like '/chat/%'  then 'chat'
        when pages.path like '/consult%' then 'video'
        else 'error'
      end as activity
    , case
        when pages.path like '/consult/%' then split_part(pages.path, '/'::text, 3)
        when pages.path like '/chat/%' and pages.path like '%channelId%'
            then split_part(split_part(pages.path, '/'::text, 3), ';'::text, 1)
        when pages.path like '/chat/%' and pages.path not like '%channelId%'
             then split_part(pages.path, '/'::text, 3)
        else null
      end as patient_id
    , case
        when pages.path like '/consult/%' then split_part(pages.path, '/'::text, 4)
        when pages.path like '/chat/%' and pages.path like '%channelId%'
            then split_part(pages.path, '='::text, 2)
        when pages.path like '/chat/%' and pages.path not like '%channelId%'
            and split_part(pages.path, '/'::text, 4) <> ''
            then split_part(pages.path, '/'::text, 4)
        else pages.episode_id
      end as episode_id
    , coalesce(practitioners.main_specialization, 'N/A') as main_specialization
from careplatform_pages as pages
inner join practitioners using (user_id)
where date_trunc('day', timestamp) >= '2018-01-01'
