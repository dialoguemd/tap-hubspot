with
	ranked as (
		select common_clicks as clicks
			, common_cost/1000::float as cost
			, currency
			, common_impressions as impressions
			, channel
			, event as platform
			, date_trunc('day', timestamp) as date_day
			, row_number() over (partition by channel, date_trunc('day', timestamp)
				order by date_trunc('day', timestamp)) as rank
		from funnelio.linked_in
	)

select *
from ranked
where rank = 1
