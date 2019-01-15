with
	model_runs as (
		select * from {{ ref('_audit_model_runs') }}
	)

	, dbt_runs_tmp as (
		select invocation_id
			, min(started_at) as started_at
			, max(completed_at) as completed_at
			, extract(epoch from max(completed_at) - min(started_at))
				as duration_s
			, count(distinct event_model) as models_run_count
		from model_runs
		group by 1
	)

	, dbt_runs as (
		select invocation_id
			, started_at
			, completed_at
			, duration_s
			, models_run_count
			-- If count of models run is greater than 85% of our all time max count
			-- then assume it is a full_dbt_run
			, models_run_count * 1.0
				/ max(models_run_count) over (order by started_at) > 0.85
				as full_dbt_run
		from dbt_runs_tmp
		group by 1,2,3,4,5
	)

select *
from dbt_runs
where full_dbt_run
