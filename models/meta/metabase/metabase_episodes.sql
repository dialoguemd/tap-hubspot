-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with episodes as (
        select * from {{ ref('pdt_episodes') }}
    )

    select episodes.episode_id
        , episodes.user_id
        , users.age
        , users.gender
        , users.organization_name
        , organizations.account_name
        , organizations.account_industry
        , episodes.url_zorro
        , episodes.created_at
        , episodes.last_post_at
        , episodes.first_outcome_category
        , episodes.first_outcome
        , episodes.outcome_category
        , episodes.outcome
        , episodes.issue_type
        , episodes.first_priority_level
        , episodes.priority_level
        , episodes.rating
        , episodes.score as nps_score
        , episodes.category as nps_score_category
        , episodes.first_set_resolved_pending_at as first_set_resolved
        , episodes.ttr_total
        , episodes.attr_total
        , episodes.attr_nc_day_7
        , episodes.attr_np_day_7
        , episodes.attr_cc_day_7
        , episodes.attr_gp_day_7
    from episodes
    left join pdt.users
        on episodes.user_id = users.user_id
    left join pdt.organizations
        on users.organization_id = organizations.organization_id
