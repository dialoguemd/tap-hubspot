-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with cp_activity as (
        select * from {{ ref( 'cp_activity' ) }}
		)

  , episodes as (
        select * from {{ ref( 'episodes' ) }}
    )

  select cp_activity.*
      , issue_type
      , outcome
      , priority_level
  from cp_activity
  left join episodes using (episode_id)