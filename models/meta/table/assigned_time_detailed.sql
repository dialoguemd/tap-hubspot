
{{
  config(
    materialized='incremental',
    unique_key='assignment_id',
    post_hook=[
       "{{ postgres.index(this, 'assignment_id')}}",
    ]
  )
}}

with assigned_time as (
        select * from {{ ref('assigned_time_w_posts') }}
        {% if is_incremental() %}
        where assigned_at > (select max(assigned_at) from {{ this }})
        {% endif %}
    )

    , responses as (
        select * from {{ ref('messaging_practitioner_responses') }}
        {% if is_incremental() %}
        where created_at > (select max(assigned_at) from {{ this }})
        {% endif %}
    )

    , assigned_time_detailed as (
        select assigned_time.assignment_id
            , assigned_time.episode_id
            , assigned_time.assigned_user_id
            , assigned_time.user_id
            , assigned_time.assigned_at
            , assigned_time.unassigned_at
            , assigned_time.assigned_at_est
            , assigned_time.unassigned_at_est
            , assigned_time.assigned_range
            , assigned_time.assigned_range_est
            , assigned_time.main_specialization
            , assigned_time.first_post_at
            , assigned_time.first_response_time_min
            , assigned_time.assigned_time_min
            , assigned_time.count_posts
            , assigned_time.assignment_rank
            , sum(responses.in_chat_time) as rt_sum
            , count(responses.in_chat_time) as rt_count
        from assigned_time
        left join responses
            on assigned_time.user_id = responses.user_id
            and assigned_time.assigned_range @> responses.created_at
            and assigned_time.assigned_range @> responses.previous_post_at
        {{ dbt_utils.group_by(16) }}
    )

select assignment_id
    , episode_id
    , assigned_user_id
    , user_id
    , main_specialization
    , case when assignment_rank = 1 then 'First Assignment'
        else 'Other Assignment' end as assignment_type
    , assigned_at
    , unassigned_at
    , assigned_at_est
    , unassigned_at_est
    , assigned_range
    , assigned_range_est
    , first_post_at
    , first_response_time_min
    , assigned_time_min
    , count_posts
    , rt_sum
    , rt_count
from assigned_time_detailed
