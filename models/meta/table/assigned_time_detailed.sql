with assigned_time as (
        select * from {{ ref('assigned_time_w_posts') }}
    )

    , responses as (
        select * from {{ ref('messaging_practitioner_responses') }}
        {% if target.name == 'dev' %}
        where created_at > current_date - interval '1 weeks'
        {% else %}
        where created_at > current_date - interval '6 months'
        {% endif %}
    )

    , assigned_time_detailed as (
        select assigned_time.episode_id
            , assigned_time.assigned_user_id
            , assigned_time.user_id
            , assigned_time.assigned_at
            , assigned_time.unassigned_at
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
            and tstzrange(
                assigned_time.assigned_at,
                assigned_time.unassigned_at
                ) @> responses.created_at
            and tstzrange(
                assigned_time.assigned_at,
                assigned_time.unassigned_at
                ) @> responses.previous_post_at
        group by 1,2,3,4,5,6,7,8,9,10,11
    )

select episode_id
    , assigned_user_id
    , user_id
    , main_specialization
    , case when assignment_rank = 1 then 'First Assignment'
        else 'Other Assignment' end as assignment_type
    , assigned_at
    , unassigned_at
    , first_post_at
    , first_response_time_min
    , assigned_time_min
    , count_posts
    , rt_sum
    , rt_count
from assigned_time_detailed
