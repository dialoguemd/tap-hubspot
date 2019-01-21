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
    , null as sdr_id
    , null as sdr_name
    , null as sdr_title
from activities_detailed

{% for status in ["Meeting", "Initiate",
    "Educate", "Validate", "Justify", "Decide"]
-%}

union all

select
    {% if status == "Meeting" -%}
        'Meeting Booked'
    {%- else -%}
        '{{ status }}'
    {%- endif %} as status
    , opportunity_id
    , {{ status.lower() }}_date as date
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
    , sdr_id
    , sdr_name
    , sdr_title
from opportunities_direct
where {{ status.lower() }}_date is not null

{% endfor -%}

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
    , sdr_id
    , sdr_name
    , sdr_title
from opportunities_direct
where is_closed and is_won
