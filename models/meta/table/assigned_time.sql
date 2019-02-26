with assignments as (
        select * from {{ ref('usher_episode_assigned') }}
    )

	, state_changes as (
		select * from {{ ref('usher_episode_state_updated') }}
	)

	, unioned as (
        select episode_id
            , assigned_user_id
            , user_id
            , assigned_at
            , date_trunc('day', timezone('America/Montreal', assigned_at)) as date_day_est
        from assignments
        union all
        select episode_id
            , null as assigned_user_id
            , user_id
            , timestamp as assigned_at
            , date_trunc('day', timezone('America/Montreal', timestamp)) as date_day_est
        from state_changes
        where episode_state in ('pending', 'resolved')
	)

    , assignments_tmp as (
        select *
            -- Resolve messages are preceeded by an unassign and are themselves 
            -- not assigned to anyone
            , lag(assigned_user_id) over
                (partition by episode_id, date_day_est order by assigned_at) is null
                and assigned_user_id is null as resolve_message
            -- Only included unassigns if they occur on the same day
            , lead(assigned_at) over
                (partition by episode_id, date_day_est order by assigned_at) as unassigned_at
            , rank() over (partition by episode_id order by assigned_at) as rank
        from unioned
    )

select -- use MD5 to make an elegant assignment_id
    md5(episode_id || assigned_user_id || assigned_at) as assignment_id
    , episode_id
    , assigned_user_id
    , user_id
    , assigned_at
    , unassigned_at
    , date_day_est
from assignments_tmp
where (resolve_message is false
    or rank = 1)
    and assigned_user_id is not null
    -- Don't take any assignments from today because they may not have
    -- unassigned_at timestamps
    and date_day_est <
        date_trunc('day', timezone('America/Montreal', current_timestamp))
