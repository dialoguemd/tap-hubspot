with
	opportunity_history as (
		select * from {{ ref('salesforce_opportunity_history') }}
	)

	, opportunity_history_rank as (
		select stage_name in ('Long Term', 'Stalled') as is_stalled
			, lead(stage_name in ('Long Term', 'Stalled'))
				over(
					partition by opportunity_id
					order by created_date
				) as next_is_stalled
			, lead(created_date)
				over(
					partition by opportunity_id
					order by created_date
				) as next_created_date
			, *
		from opportunity_history
	)

select opportunity_id
	, tstzrange(
		created_date,
		coalesce(next_created_date, '9999-12-31 23:59:59.999999')
	) as stalled_range
from opportunity_history_rank
where is_stalled
