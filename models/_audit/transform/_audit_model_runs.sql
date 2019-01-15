with
	audit as (
		select * from {{ ref('_audit_dbt_audit_log') }}
	)

	, started as (
		select event_timestamp as started_at
			, event_model
			, invocation_id
		from audit
		where event_name = 'starting model run'
	)

	, completed as (
		select event_timestamp as completed_at
			, event_model
			, invocation_id
		from audit
		where event_name = 'completed model run'
	)

	, model_runs as (
		select started.invocation_id
			, started.event_model
			, started.started_at
			, completed.completed_at
			, extract(epoch from completed.completed_at - started.started_at)
				as duration_s
			, row_number()
				over (partition by started.event_model
				order by started.started_at desc) as rank_reverse
		from started
		left join completed
			using (event_model, invocation_id)
	)

select invocation_id
	, event_model
	, started_at
	, completed_at
	, duration_s
	, rank_reverse = 1 as most_recent_run
from model_runs
