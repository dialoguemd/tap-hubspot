with
    episodes as (
        select * from {{ ref('episodes') }}
    )

    , users as (
        select * from {{ ref('scribe_users_detailed') }}
    )

    , practitioners as (
        select * from {{ ref('coredata_practitioners') }}
    )

    , episode_stats as (
        select user_id
            , count(*) as count_episodes
            , sum(messages_total) as count_messages_total
            , sum(messages_patient)::real as count_messages_sent
            , avg(score) as avg_nps_score
            , avg(rating) as avg_5_star_rating
            , min(first_message_patient) as activated_at
        from episodes
        where first_message_patient is not null
        group by 1
    )

select users.user_id as patient_id
    , users.age
    , users.gender
    , users.language
    , users.residence_province
    , users.status
    , users.created_at as invited_at
    , users.signed_up_at
    , episode_stats.activated_at
    , users.family_id is not null as family_registered
    , users.is_child
    , users.is_signed_up
    , case when users.is_signed_up
        then users.user_id
        else null
        end as signed_up_user_id
    , episode_stats.activated_at is not null as is_activated
    , case when episode_stats.activated_at is not null
        then users.user_id
        else null
        end as activated_user_id
    , episode_stats.count_episodes
    , episode_stats.count_messages_total
    , episode_stats.count_messages_sent
    , episode_stats.avg_nps_score
    , episode_stats.avg_5_star_rating
    , case when users.signed_up_at < users.created_at
           then 0
           else extract(epoch from users.signed_up_at - users.created_at)/3600
           end as time_to_sign_up_hours
    , case when episode_stats.activated_at < users.signed_up_at
           then 0
           else extract(epoch from episode_stats.activated_at - users.signed_up_at)/3600
           end as time_to_activate_hours
from users
left join episode_stats
    using (user_id)
left join practitioners
    using (user_id)
where practitioners.user_id is null
