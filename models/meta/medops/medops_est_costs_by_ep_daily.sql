with priced_episodes as (
        select * from {{ ref( 'medops_priced_episodes' ) }}
    )
    
    , priced_videos as (
        select * from {{ ref( 'medops_priced_videos' ) }}
    )

    , episodes as (
        select * from {{ ref( 'episodes' ) }}
    )

select coalesce(priced_episodes.episode_id, priced_videos.episode_id) as episode_id
    , episodes.patient_id as user_id
    , coalesce(priced_episodes.date, priced_videos.date) as date_day
    , coalesce(priced_episodes.cc_cost,0) as cc_cost
    , coalesce(priced_episodes.nc_cost,0) as nc_cost
    , coalesce(priced_episodes.np_cost,0) as np_cost
    , coalesce(priced_videos.gp_psy_cost,0) as gp_psy_cost
    , coalesce(priced_videos.gp_other_cost,0) as gp_other_cost
from priced_episodes
-- Full outer for edge cases of no other ep costs on the given day
full outer join priced_videos
    on priced_episodes.episode_id = priced_videos.episode_id
    and priced_episodes.date = priced_videos.date
left join episodes
    on coalesce(priced_episodes.episode_id, priced_videos.episode_id)
        = episodes.episode_id
