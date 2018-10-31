with
    tasks as (
        select * from {{ ref('salesforce_tasks') }}
    )

    , events as (
        select * from {{ ref('salesforce_events') }}
    )

select id as activity_id
    , account_id
    , owner_id
    , status
    , coalesce(activity_date, created_date) as activity_date
    , type
    , task_subtype
from tasks
union all
select id as activity_id
    , account_id
    , owner_id
    , 'Completed' as status
    , start_date_time as activity_date
    , type
    , event_subtype as task_subtype
from events
