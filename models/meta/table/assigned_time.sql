with assignments as (
        select * from {{ ref('usher_episode_assigned') }}
    )

	, snoozed as (
		select * from {{ ref('careplatform_snooze_started') }}
	)

	, state_changes as (
		select * from {{ ref('usher_episode_state_updated') }}
	)

	, unioned as (
        select episode_id
            , assigned_user_id
            , user_id
            , assigned_at
        from assignments
        union all
        select episode_id
            , null as assigned_user_id
            , user_id
            , timestamp as assigned_at
        from snoozed
        union all
        select episode_id
            , null as assigned_user_id
            , user_id
            , timestamp as assigned_at
        from state_changes
        where episode_state in ('pending', 'resolved')
	)

    , assignments_tmp as (
        select *
            -- Resolve messages are preceeded by an unassign and are themselves 
            -- not assigned to anyone 
            , lag(assigned_user_id) over
                (partition by episode_id order by assigned_at) is null
                and assigned_user_id is null as resolve_message
            , lead(assigned_at) over
                (partition by episode_id order by assigned_at) as unassigned_at
            , rank() over (partition by episode_id order by assigned_at) as rank
        from unioned
    )

select episode_id
    , assigned_user_id
    , user_id
    , assigned_at
    , unassigned_at
from assignments_tmp
where (resolve_message is false
    or rank = 1)
    and assigned_user_id is not null
