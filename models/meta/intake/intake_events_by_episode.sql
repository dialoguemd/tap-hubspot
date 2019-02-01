with 
	events as (
		select * from {{ ref( 'intake_events_by_episode_unfiltered' ) }}
	)

    , episodes as (
        select * from {{ ref( 'episodes' ) }}
    )

    , dxa_rank as (
        select episode_id
            , min(rank) filter (where event_name = 'dxa') as dxa_rank
            , min(event_name) filter (where rank = '1') as first_rank
        from events
        group by 1
    )

select events.*
    , dxa_rank.dxa_rank
    , episodes.cc_code
    , episodes.reason_for_visit
    , episodes.issue_type
    , episodes.outcome
from events
left join dxa_rank
    on events.episode_id = dxa_rank.episode_id
left join episodes
    on events.episode_id = episodes.episode_id
where (events.rank <= dxa_rank.dxa_rank or dxa_rank.dxa_rank is null)
    -- Only include full episode event sequences i.e. don't include partials
    -- that could start on an event like dxa_permission
    and dxa_rank.first_rank = 'top_level_greeting'
