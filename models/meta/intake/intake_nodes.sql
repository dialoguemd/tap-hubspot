with
    intake_events as (
        select * from {{ ref('intake_events_by_episode') }}
        where dxa_rank = 6
    )

    , nodes_tmp as (
        select event_name as previous_event
        	, following_event
        	, type
        	, count(*)
        from intake_events
        where timestamp > current_date - interval '2 weeks'
            and ((rank = 2 and event_name = 'location_serviceability')
                or (rank = 3 and event_name = 'episode_subject_and_reason')
                or rank not in (1,2))
        group by 1,2,3
    )

    , nodes as (
		select coalesce(previous_event, 'episode_start') as previous_event
		  , coalesce(type, 'qnaire') as type
		  , case when coalesce(type, 'qnaire') = 'free_text' then 'b' else 'r' end as type_colour
		  , coalesce(following_event, 'end_of_intake') as following_event
		  , count
		  , round(count / sum(count) over (partition by previous_event) * 1.0, 2) as fraction
		from nodes_tmp
	)

select previous_event
	, following_event
	, type
	, type_colour
	, fraction
from nodes
