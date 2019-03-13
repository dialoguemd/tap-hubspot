with
    episodes as (
        select * from {{ ref('episodes') }}
    )

    , costs_by_episode_daily as (
        select * from {{ ref('costs_by_episode_daily') }}
    )

    , episode_cost as (
        select episodes.episode_id
            , sum(total_cost) filter(where
                costs_by_episode_daily.date_day
                < episodes.first_message_patient + interval '7 days'
            ) as costs_7_days
        from episodes
        left join costs_by_episode_daily
            using (episode_id)
        where date_trunc('day', episodes.first_message_patient)
            = current_date - interval '8 days'
        group by 1
    )

select avg(costs_7_days) as costs_7_days_avg
from episode_cost
