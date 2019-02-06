with
	unresponsive_nudge as (
		select * from {{ ref('unresponsive_nudge') }}
	)

	, nudge as (
		select episode_id
			, timestamp
			, timezone('America/Montreal', timestamp) as timestamp_est
			, date_trunc('day', timestamp) as date_day
			, date_trunc('day', timezone('America/Montreal', timestamp))
				as date_day_est
		from unresponsive_nudge
	)

select episode_id
	, date_day_est
	, min(timestamp_est) as first_timestamp_est
	, max(timestamp_est) as last_timestamp_est
	, count(*) as nudge_count
from nudge
group by 1,2
