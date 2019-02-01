-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with episodes as (
		select * from {{ ref('episodes_with_contracts') }}
	)

	, user_contract as (
		select * from {{ ref('user_contract') }}
	)

	select episodes.episode_id
		, episodes.user_id
		, extract('year'
			from age(episodes.created_at, user_contract.birthday)) as age
		, user_contract.gender
		, user_contract.organization_name
		, user_contract.account_name
		, user_contract.account_industry
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
	left join user_contract
		on episodes.patient_id = user_contract.user_id
		and episodes.first_message_created_at <@ user_contract.during_est
