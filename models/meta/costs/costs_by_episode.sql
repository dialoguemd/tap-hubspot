with time_spent as (
        select * from {{ ref( 'costs_time_spent_by_episode_daily' ) }}
    )

    , hourly_cost as (
        select * from {{ ref( 'costs_hourly_by_spec_monthly' ) }}
    )

    , chats as (
        select * from {{ ref( 'chats' ) }}
    )

    , episodes as (
        select * from {{ ref( 'episodes' ) }}
    )

select time_spent.episode_id
    , time_spent.date
    , episodes.patient_id
    , chats.chat_type
    , time_spent.cc_time * hourly_cost.cc_hourly as cc_cost
    , time_spent.nc_time * hourly_cost.nc_hourly as nc_cost
    , time_spent.np_time * hourly_cost.np_hourly as np_cost
    , time_spent.cc_time
    , time_spent.nc_time
    , time_spent.np_time
from time_spent
left join chats
    on time_spent.episode_id = chats.episode_id
    and time_spent.date = chats.date_day_est
left join episodes
    on time_spent.episode_id = episodes.episode_id
inner join hourly_cost
	on date_trunc('month', time_spent.date)
        = hourly_cost.date_month
