with
	episodes as (
		select * from {{ ref('episodes') }}
	)

	, qnaires as (
		select * from {{ ref('countdown_qnaire_completion_stats') }}
	)

select qnaires.*
	, episodes.issue_type
	, episodes.outcome
from qnaires
left join episodes
	using (episode_id)
