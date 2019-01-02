with
	episodes_subject as (
		select * from {{ ref('episodes_subject') }}
	)

select date_week
    , 1.0 * count(*) filter(where episode_subject is null)
        / count(*) as null_rate
    , count(*) filter(where episode_subject is null) as null_count
    , count(*) as episode_count
from episodes_subject
-- exclude weeks if we're only 2 days in
where date_week < current_timestamp - interval '2 days'
group by 1
having 1.0 * count(*) filter(where episode_subject is null)
    / count(*) > .2
