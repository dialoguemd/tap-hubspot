with
	episodes as (
		select * from {{ ref('episodes') }}
	)

	, aggregate as (
		select date_week_est
		    , 1.0 * count(*) filter(where sla_answered_within_15_minutes)
		         / count(sla_answered_within_15_minutes) as percentage_within_sla
		    , 1.0 * count(sla_answered_within_15_minutes)
		         / count(*) as percentage_with_valid_sla
		    , count(*) as chats_count
		from episodes
		where  date_week_est >= '2018-06-04'
			and date_week_est < date_trunc('week', current_date)
		group by 1
	)

select *
from aggregate
where (
		percentage_within_sla < .83
		or percentage_with_valid_sla < .66
	) and date_trunc('week', current_date) > date_week_est
