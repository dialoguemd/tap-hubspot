with
    opportunities_direct as (
        select * from {{ ref('salesforce_opportunities_detailed_direct') }}
    )

    , activities_detailed as (
        select * from {{ ref('salesforce_activities_detailed') }}
    )

select 'Activity' as status
    , activity_id as opportunity_id
    , activity_date as date
    , coalesce(meeting_date, activity_date) as meeting_date
    , null as close_date
    , null as is_closed
    , account_is_won as is_won
    , null as age_in_days
    , number_of_employees
    , null as segment
    , owner_name
    , null as owner_title
    , amount
    , province
    , country
    , industry
    , null as lead_source
    , false as is_inbound
from activities_detailed
union all
select 'Meeting Booked' as status
    , opportunity_id
    , meeting_date as date
    , meeting_date
    , close_date
    , is_closed
    , is_won
    , opportunity_age as age_in_days
    , number_of_employees
    , segment
    , owner_name
    , owner_title
    , amount
    , province
    , country
    , industry
    , lead_source
    , is_inbound
from opportunities_direct
where meeting_date is not null
union all
select 'Initiate' as status
    , opportunity_id
    , initiate_date as date
    , meeting_date
    , close_date
    , is_closed
    , is_won
    , opportunity_age as age_in_days
    , number_of_employees
    , segment
    , owner_name
    , owner_title
    , amount
    , province
    , country
    , industry
    , lead_source
    , is_inbound
from opportunities_direct
where initiate_date is not null
union all
select 'Educate' as status
    , opportunity_id
    , educate_date as date
    , meeting_date
    , close_date
    , is_closed
    , is_won
    , opportunity_age as age_in_days
    , number_of_employees
    , segment
    , owner_name
    , owner_title
    , amount
    , province
    , country
    , industry
    , lead_source
    , is_inbound
from opportunities_direct
where educate_date is not null
union all
select 'Validate' as status
    , opportunity_id
    , validate_date as date
    , meeting_date
    , close_date
    , is_closed
    , is_won
    , opportunity_age as age_in_days
    , number_of_employees
    , segment
    , owner_name
    , owner_title
    , amount
    , province
    , country
    , industry
    , lead_source
    , is_inbound
from opportunities_direct
where validate_date is not null
union all
select 'Justify' as status
    , opportunity_id
    , justify_date as date
    , meeting_date
    , close_date
    , is_closed
    , is_won
    , opportunity_age as age_in_days
    , number_of_employees
    , segment
    , owner_name
    , owner_title
    , amount
    , province
    , country
    , industry
    , lead_source
    , is_inbound
from opportunities_direct
where justify_date is not null
union all
select 'Decide' as status
    , opportunity_id
    , decide_date as date
    , meeting_date
    , close_date
    , is_closed
    , is_won
    , opportunity_age as age_in_days
    , number_of_employees
    , segment
    , owner_name
    , owner_title
    , amount
    , province
    , country
    , industry
    , lead_source
    , is_inbound
from opportunities_direct
where decide_date is not null
union all
select 'Closed Won' as status
    , opportunity_id
    , close_date as date
    , meeting_date
    , close_date
    , is_closed
    , is_won
    , opportunity_age as age_in_days
    , number_of_employees
    , segment
    , owner_name
    , owner_title
    , amount
    , province
    , country
    , industry
    , lead_source
    , is_inbound
from opportunities_direct
where is_closed and is_won
