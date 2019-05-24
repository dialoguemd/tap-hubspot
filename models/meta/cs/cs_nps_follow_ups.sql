
{{
  config(
    materialized='incremental',
    unique_key='survey_id',
    post_hook=[
       "{{ postgres.index(this, 'survey_id')}}",
    ]
  )
}}

with conversations as (
        select * from {{ ref('intercom_conversations_detailed') }}
    )

    , nps_survey as (
        select * from {{ ref('nps_patient_survey') }}
    )

    , working_minutes as (
        select * from {{ ref('dimension_working_minutes') }}
    )

    , nps_responses as (
        select nps_survey.survey_id
            , nps_survey.user_id
            , nps_survey.score
            , nps_survey.category
            , nps_survey.tags
            , nps_survey.contract_id
            , nps_survey.organization_name
            , nps_survey.timestamp as nps_completed_at
            , tstzrange(nps_survey.updated_at, nps_survey.updated_at + interval '21 days') as follow_up_range
        from nps_survey
        {% if is_incremental() %}
        where timestamp > (select max(nps_completed_at) from {{ this }})
        {% endif %}
    )

    , follow_ups as (
        select nps_responses.survey_id
            , nps_responses.score
            , nps_responses.category
            , nps_responses.contract_id
            , nps_responses.organization_name
            , timezone('America/Montreal', nps_responses.nps_completed_at)
                as nps_completed_at
            , tsrange(
                timezone('America/Montreal', nps_responses.nps_completed_at),
                timezone('America/Montreal', min(conversations.conversation_started))
                ) as time_to_respond
            , timezone('America/Montreal', 
                min(conversations.conversation_started))
                as admin_first_message
            , timezone('America/Montreal', 
                min(conversations.user_first_message))
                as user_first_message
        from nps_responses
        left join conversations
            on nps_responses.follow_up_range @> conversations.conversation_started
            and nps_responses.user_id = conversations.user_id
        {{ dbt_utils.group_by(6) }}
    )

select follow_ups.survey_id
    , follow_ups.score
    , follow_ups.category
    , follow_ups.contract_id
    , follow_ups.organization_name
    , follow_ups.nps_completed_at
    , follow_ups.admin_first_message
    , follow_ups.user_first_message
    , extract(epoch from follow_ups.admin_first_message
        - follow_ups.nps_completed_at)/3600 :: float
        as time_to_follow_up_calendar
    , case when follow_ups.admin_first_message is not null
        then count(working_minutes.date_minute)/60 :: float
        end as time_to_follow_up_business
from follow_ups
left join working_minutes
    on follow_ups.time_to_respond
        @> working_minutes.date_minute
    and follow_ups.admin_first_message is not null
{{ dbt_utils.group_by(9) }}
