with
	dxa_dx as (
		select * from {{ ref('dxa_dx') }}
	)

	, qnaire_started as (
		select * from {{ ref('countdown_qnaire_started') }}
	)

select dxa_dx.*
	, qnaire_started.episode_id
	, qnaire_started.timestamp
from dxa_dx
inner join qnaire_started
	using (qnaire_tid)
