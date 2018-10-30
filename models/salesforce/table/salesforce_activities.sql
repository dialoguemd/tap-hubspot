select id as activity_id
    , account_id
    , owner_id
    , status
    , activity_date
    , type
    , task_subtype
from {{ ref('salesforce_tasks') }}
union all
select id as activity_id
    , account_id
    , owner_id
    , 'Completed' as status
    , start_date_time as activity_date
    , type
    , event_subtype as task_subtype
from {{ ref('salesforce_events') }}
