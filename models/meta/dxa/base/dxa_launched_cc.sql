with messaging as (
        select * from {{ ref('messaging_dxa_cc') }}
    )

    , mm as (
        select * from {{ ref('mm_dxa_cc') }}
    )

    , unioned as (
		select episode_id
			, timestamp
			, cc
		from messaging
		union all
		select episode_id
			, timestamp
			, cc
		from mm
	)

    , ranked as (
		select md5(episode_id || timestamp) as dxa_launched_id
			, episode_id
			, timestamp
			, cc
			, row_number() over (partition by episode_id, timestamp) as rank
		from unioned
	)

select *
from ranked
where rank = 1
