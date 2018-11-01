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
        select * from {{ ref( 'pdt_episodes' ) }}
    )

    select organization_name
        , date_trunc('month', priced_episodes.date) as month
        , coalesce(cc_cost,0) as cc_cost
        , coalesce(nc_cost,0) as nc_cost
        , coalesce(np_cost,0) as np_cost
        , coalesce(gp_psy_cost,0) as gp_psy_cost
        , coalesce(gp_other_cost,0) as gp_other_cost
    from priced_episodes
    left join priced_videos
        on priced_episodes.episode_id = priced_videos.episode_id
        and priced_episodes.date = priced_videos.date
    left join episodes
        on priced_episodes.episode_id = episodes.episode_id
