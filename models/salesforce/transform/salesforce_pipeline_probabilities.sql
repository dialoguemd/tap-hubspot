select segment
    , status as stage_name
    , 1.0 * count(*) filter(where is_won) / count(*) as probability
    , count(*) as opportunities_count
from {{ ref('salesforce_opportunities_all_stages') }}
where meeting_date >= '2018-01-01'
    and meeting_date <= '2018-07-01'
    and status not in ('Closed Won', 'Activity')
group by 1,2
