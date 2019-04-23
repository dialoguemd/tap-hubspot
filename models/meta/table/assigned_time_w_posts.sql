{{
  config(
    materialized='incremental',
    unique_key='assignment_id',
    post_hook=[
       "DROP INDEX IF EXISTS {{ this.schema }}.index_assigned_w_posts_assigned_at",
       "CREATE INDEX IF NOT EXISTS index_assigned_w_posts_assigned_at ON {{ this }}(assigned_at)"
    ]
  )
}}

with assigned_time as (
        select * from {{ ref('assigned_time') }}
        where assigned_at > current_date - interval '6 months'
        -- Filtering for incremental model
        {% if is_incremental() %}
            and assigned_at > (select max(assigned_at) from {{ this }})
        {% endif %}
    )

    , posts as (
        select * from {{ ref('messaging_posts_all_time') }}
        where created_at > current_date - interval '6 months'
        -- Filtering for incremental model
        {% if is_incremental() %}
            and created_at > (select max(assigned_at) from {{ this }})
        {% endif %}
    )

    , practitioners as (
        select * from {{ ref('coredata_practitioners') }}
    )

    , assignments as (
        select assignment_id
            , episode_id
            , assigned_user_id
            , user_id
            , assigned_at
            , least(
                unassigned_at,
                date_trunc('day', assigned_at + interval '1 day')
            ) as unassigned_at
        from assigned_time
    )

select assignments.assignment_id
    , assignments.episode_id
    , assignments.assigned_user_id
    , assignments.user_id
    , assignments.assigned_at
    , assignments.unassigned_at
    , timezone('America/Montreal', assignments.assigned_at)
        as assigned_at_est
    , timezone('America/Montreal', assignments.unassigned_at)
        as unassigned_at_est
    , tstzrange(
        assignments.assigned_at,
        assignments.unassigned_at
        ) as assigned_range
    , tsrange(
        timezone('America/Montreal', assignments.assigned_at),
        timezone('America/Montreal', assignments.unassigned_at)
        ) as assigned_range_est
    , coalesce(practitioners.main_specialization, 'N/A')
        as main_specialization
    , min(posts.created_at) as first_post_at
    , extract(epoch from min(posts.created_at)
        - assignments.assigned_at)/60 as first_response_time_min
    , extract(epoch from assignments.unassigned_at
        - assignments.assigned_at)/60 as assigned_time_min
    , count(posts.*) as count_posts
    , row_number() over (partition by assignments.episode_id,
        practitioners.main_specialization order by assignments.assigned_at)
        as assignment_rank
from assignments
left join posts
    on assignments.user_id = posts.user_id
    and is_internal_post is false
    and tstzrange(
        assignments.assigned_at,
        assignments.unassigned_at
        ) @> posts.created_at
left join practitioners on assignments.user_id = practitioners.user_id
group by 1,2,3,4,5,6,practitioners.main_specialization
