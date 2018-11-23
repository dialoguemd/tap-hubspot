with assignments as (
        select * from {{ ref('assigned_time') }}
        {% if target.name == 'dev' %}
        where assigned_at > current_date - interval '4 weeks'
        {% endif %}
    )

    , posts as (
        select * from {{ ref('messaging_posts_all_time') }}
        {% if target.name == 'dev' %}
        where created_at > current_date - interval '4 weeks'
        {% endif %}
    )

    , practitioners as (
        select * from {{ ref('coredata_practitioners') }}
    )

    , ranked as (
        select assignments.episode_id
            , assignments.assigned_user_id
            , assignments.user_id
            , assignments.assigned_at
            , assignments.unassigned_at
            , coalesce(practitioners.main_specialization, 'N/A')
                as main_specialization
            , min(posts.created_at) as first_post_at
            , extract(epoch from min(posts.created_at)
                - min(assignments.assigned_at))/60 as first_response_time_min
            , extract(epoch from min(assignments.unassigned_at)
                - min(assignments.assigned_at))/60 as dispatch_time_min
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
        group by 1,2,3,4,5,practitioners.main_specialization
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
    , dispatch_time_min
    , count_posts
from ranked
