with careplatform_pages as (
    select * from {{ ref( 'careplatform_pages' ) }}
)

select pages.user_id
    , date_trunc('day', timezone('America/Montreal',timestamp)) as date
    , timezone('America/Montreal', timestamp) as timestamp
    , case
        when path in ('/','/pending', '/resolved', '/care-plans', '/reminders', '/mentions', '/login','/reminders/completed','/snoozed') then 'dashboard'
        when path like '/chat/%'  then 'chat'
        when path like '/consult%' then 'video'
        else 'error'
      end as activity
    , case
        when path like '/consult/%' then split_part(path, '/'::text, 3)
        when path like '/chat/%' and path like '%channelId%' then split_part(split_part(path, '/'::text, 3), ';'::text, 1)
        when path like '/chat/%' and path not like '%channelId%' then split_part(path, '/'::text, 3)
        else null
      end as patient_id
    , case
        when path like '/consult/%' then split_part(path, '/'::text, 4)
        when path like '/chat/%' and path like '%channelId%' then split_part(path, '='::text, 2)
        when path like '/chat/%' and path not like '%channelId%' and split_part(path, '/'::text, 4) <> '' then split_part(path, '/'::text, 4)
        else episode_id
      end as episode_id
from careplatform_pages as pages
where date_trunc('day', timezone('America/Montreal', timestamp)) >= '2018-01-01'
