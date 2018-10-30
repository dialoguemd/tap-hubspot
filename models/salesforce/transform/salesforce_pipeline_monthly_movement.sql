with
	pipeline as (
		select *
		from {{ ref('salesforce_pipeline_weighted_monthly') }}
	)

	, pipeline_lag as (
		select pipeline.*
			, last_month.amount as amount_last_month
			, last_month.amount_weighted as amount_weighted_last_month
			, next_month.amount as amount_next_month
			, next_month.amount_weighted as amount_weighted_next_month
		from pipeline
		left join pipeline as last_month
			on pipeline.opportunity_id = last_month.opportunity_id
				and pipeline.date_month - interval '1 month' = last_month.date_month
		left join pipeline as next_month
			on pipeline.opportunity_id = next_month.opportunity_id
				and pipeline.date_month + interval '1 month' = next_month.date_month
	)

	, pipeline_tmp as (
		select *
			, case
				when is_last_month_in_pipeline
					and is_first_month_in_pipeline
					and is_won
				then 'New and Closed Won'
				when is_last_month_in_pipeline
					and is_first_month_in_pipeline
					and not is_won
				then 'New and Closed Lost'
				when is_first_month_in_pipeline
					and amount_weighted_next_month is null
					and date_month <> date_trunc('month', current_date)
				then 'New and Stalled'
				when amount_weighted_last_month is null
					and is_last_month_in_pipeline
					and is_won
				then 'Closed Won from Stalled'
				when amount_weighted_last_month is null
					and is_last_month_in_pipeline
					and not is_won
				then 'Closed Lost from Stalled'
				when is_last_month_in_pipeline and is_won
				then 'Closed Won'
				when is_last_month_in_pipeline and not is_won
				then 'Closed Lost'
				when is_first_month_in_pipeline
				then 'New'
				when amount_weighted_last_month is null
					and not is_first_month_in_pipeline
				then 'Unstalled'
				when amount_weighted_next_month is null
					and date_month <> date_trunc('month', current_date)
				then 'Stalled'
				when amount_weighted = amount_weighted_last_month
				then 'Stable'
				when amount_weighted > amount_weighted_last_month
				then 'Moved forward'
				else 'N/A'
			end as movement
		from pipeline_lag
	)

	select *
		, case
			when movement in ('New and Closed Won', 'New and Closed Lost',
				'Closed Won', 'Closed Lost', 'Stalled', 'New and Stalled')
			then -amount_weighted
			when movement in ('New', 'Unstalled')
			then amount_weighted
			when movement in ('Stable', 'Closed Lost from Stalled')
			then 0
			when movement = 'Moved forward'
			then amount_weighted - amount_weighted_last_month
			else 10000000
			end as amount_delta
	from pipeline_tmp
