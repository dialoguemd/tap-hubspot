-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with cp_activity as (
        select * from {{ ref( 'pdt_cp_activity' ) }}
		)

  , episodes as (
        select * from {{ ref( 'pdt_episodes' ) }}
    )

  select cp_activity.*
      , issue_type
      , outcome
      , priority_level
  from cp_activity
  left join episodes using (episode_id)