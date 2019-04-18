with
	call_started as (
        select * from {{ ref('telephone_call_started') }}
    )

    , call_ended as (
        select * from {{ ref('telephone_call_ended')}}
    )

select call_started.call_id
	, call_started.user_id
	, call_started.episode_id
	{% for timeframe in ['day', 'week', 'month'] %}
	, date_trunc('{{timeframe}}', call_started.timestamp) as date_{{timeframe}}
	{% endfor %}
	, call_started.timestamp as started_at
	, call_ended.timestamp as ended_at
	, call_started.timestamp_est as started_at_est
	, call_ended.timestamp_est as ended_at_est
	, tstzrange(call_started.timestamp, call_ended.timestamp) as call_range
	, case when call_ended.timestamp is null
		then null
		else extract(epoch
			from  call_ended.timestamp - call_started.timestamp) / 60
		end as call_duration
from call_started
left join call_ended
	using (call_id)
