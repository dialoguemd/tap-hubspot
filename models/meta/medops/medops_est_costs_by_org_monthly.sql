-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with priced_episodes as (
        select * from {{ ref( 'medops_priced_episodes' ) }}
    )
    
    , priced_videos as (
        select * from {{ ref( 'medops_priced_videos' ) }}
    )

    , episodes as (
        select * from {{ ref( 'episodes' ) }}
    )

    , organizations as (
        select * from {{ ref( 'organizations' ) }}
    )

    select episodes.organization_name
        , organizations.account_name
        , date_trunc('month',
            coalesce(priced_episodes.date, priced_videos.date)
            ) as month
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
        on priced_episodes.episode_id = episodes.episode_id
    left join organizations using (organization_id)
