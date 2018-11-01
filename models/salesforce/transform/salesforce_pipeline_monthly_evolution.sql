with
	pipeline as (
		select date_month
			, mom_evolution
			, sum(amount) as amount
			, sum(amount_weighted) as amount_weighted
		from {{ ref('salesforce_pipeline_weighted_monthly') }}
		group by 1,2
	)

	, pipeline_lag as (
		select *
			, lag(amount) over(
				partition by mom_evolution
				order by date_month
			) as amount_last_month
			, lag(amount_weighted) over(
				partition by mom_evolution
				order by date_month
			) as amount_weighted_last_month
		from pipeline
	)

select *
	, case
		when mom_evolution = 'New'
		then amount_weighted
		when mom_evolution in ('Closed Won', 'Closed Lost')
		then - amount_weighted_last_month
		else amount_weighted - amount_weighted_last_month
	end as amount_weighted_delta
	, case
		when mom_evolution = 'New'
		then amount
		when mom_evolution in ('Closed Won', 'Closed Lost')
		then - amount
		else 0
	end as amount_delta
from pipeline_lag
