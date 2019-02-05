with
	state_changed as (
		select * from {{ ref('usher_episode_state_updated') }}
	)

	, snooze_finished as (
		select * from {{ ref('unresponsive_snooze_workflow_finished') }}
	)

	, set_episode_state as (
		select episode_id
			, timestamp_est
			, date_day_est
			, episode_state
		from state_changed
		union all
		select episode_id
			, timestamp_est
			, date_day_est
			, 'resolved' as episode_state
		from snooze_finished
	)

select episode_id
	, date_day_est
	, min(timestamp_est) filter (where episode_state = 'active')
		as first_set_active
	, min(timestamp_est) filter (where episode_state in ('pending', 'resolved'))
		as first_set_resolved_pending_at
	, min(timestamp_est) filter (where episode_state in ('pending', 'resolved'))
		is not null as set_resolved_pending

from set_episode_state
group by 1,2
