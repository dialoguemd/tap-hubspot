{% set roles = ['cc', 'nc', 'np'] %}

with
	priced_episodes as (
		select * from {{ ref('costs_by_episode') }}
	)

	, priced_videos as (
		select * from {{ ref('costs_by_video') }}
	)

	, episodes_subject as (
		select * from {{ ref('episodes_subject') }}
	)

	, episodes_issue_types as (
		select * from {{ ref('episodes_issue_types') }}
	)

select coalesce(
		priced_episodes.episode_id,
		priced_videos.episode_id
		) as episode_id
	, episodes_subject.episode_subject as user_id
	, episodes_issue_types.issue_type
	, coalesce(priced_episodes.date, priced_videos.date) as date_day

	{% for role in roles %}

	, coalesce(priced_episodes.{{role}}_cost,0) as {{role}}_cost
	, coalesce(priced_episodes.{{role}}_cost_ops,0) as {{role}}_cost_ops

	{% endfor %}

	, coalesce(priced_videos.gp_other_cost + priced_videos.gp_psy_cost, 0)
		as gp_cost_ops
	, coalesce(priced_videos.gp_other_cost + priced_videos.gp_psy_cost, 0)
		as gp_cost
	, coalesce(priced_videos.gp_psy_cost,0) as gp_psy_cost
	, coalesce(priced_videos.gp_other_cost,0) as gp_other_cost
	, (
		{% for role in roles %}

		coalesce(priced_episodes.{{role}}_cost,0) +
		
		{% endfor %}

		coalesce(priced_videos.gp_psy_cost,0) +
		coalesce(priced_videos.gp_other_cost,0)
	) as total_cost
	, (

		{% for role in roles %}

		coalesce(priced_episodes.{{role}}_cost_ops,0) +
		
		{% endfor %}

		coalesce(priced_videos.gp_psy_cost,0) +
		coalesce(priced_videos.gp_other_cost,0)) as total_cost_ops
from priced_episodes
-- Full outer for edge cases of no other ep costs on the given day
full outer join priced_videos
	on priced_episodes.episode_id = priced_videos.episode_id
	and priced_episodes.date = priced_videos.date
left join episodes_issue_types
	on coalesce(priced_episodes.episode_id, priced_videos.episode_id)
		= episodes_issue_types.episode_id
left join episodes_subject
	on coalesce(priced_episodes.episode_id, priced_videos.episode_id)
		= episodes_subject.episode_id
