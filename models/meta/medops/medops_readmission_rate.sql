with episodes as (
        select * from {{ ref ( 'episodes' ) }}
    )

    , chats_all_time as (
        select * from {{ ref ( 'chats' ) }}
    )

    , chats as (
        select episode_id
            , chat_type
            , date_day_est
            , invalid_outcomes
            , includes_patient_message
            , lag(chat_type)
                over (partition by episode_id order by date_day_est)
                as previous_chat_type
            , lag(includes_patient_message)
                over (partition by episode_id order by date_day_est)
                as previous_includes_pm
        from chats_all_time
        group by 1,2,3,4,5
    )

    , readmissions as (
        select chats.episode_id
            , case 
                when chats.chat_type = 'Other initiated by patient'
                    and chats.invalid_outcomes = 'patient_thanks'
                    then 'Patient thanks'
                when chats.chat_type = 'Other initiated by patient'
                    and chats.previous_chat_type = 'Follow-up'
                    and not chats.previous_includes_pm
                    then 'Follow-up'
                when chats.chat_type = 'Other initiated by patient'
                    then 'Readmission by patient'
                else chats.chat_type
                end as chat_type
            , chats.date_day_est
            , extract(epoch 
                from age(date_trunc('day', chats.date_day_est),
                date_trunc('day', episodes.first_set_resolved_pending_at))
              )/86400 as time_since_first_resolve
        from chats
        left join episodes using (episode_id)
    )

select date_trunc('week', date_day_est) as week
    , count(*)
        filter (where chat_type = 'Readmission by patient'
            and time_since_first_resolve < 14) as readmission_count
    , count(*)
        filter (where chat_type = 'New Episode') as new_episode_count
from readmissions
group by 1
