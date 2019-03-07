with
	episodes as (
        select * from {{ ref('episodes') }}
    )

	, cp_activity as (
		select * from {{ ref('cp_activity') }}
	)

	-- , apt_booking as (
 --        select * from {{ ref('careplatform_appointment_booking_started') }}
 --    )

select episodes.episode_id
    , episodes.issue_type
    , episodes.outcome
	, episodes.user_id
	, episodes.first_message_patient
	, episodes.first_set_active
	, episodes.first_message_care_team
	, episodes.first_message_nurse
	-- , apt_booking.timestamp_est as apt_booking_at
	, episodes.first_set_resolved_pending_at
    , episodes.dxa_started_at is not null and episodes.dxa_completed_at is null as has_dxa_started_but_not_completed
    , episodes.dxa_completed_at is not null as has_dxa_completed

	, extract(epoch from
		episodes.first_set_resolved_pending_at
		- episodes.first_set_active) / 60
		as time_to_resolve_from_active
	-- , extract(epoch from
		-- apt_booking.timestamp_est
		-- - episodes.first_set_active) / 60
		-- as time_to_apt_booking_from_active
	, extract(epoch from
		episodes.first_set_resolved_pending_at
		- episodes.dxa_completed_at) / 60
		as time_to_resolve_from_dxa
	-- , extract(epoch from
	-- 	episodes.first_set_resolved_pending_at
	-- 	- episodes.dxa_completed_at) / 60
	-- 	as time_to_apt_booking_from_dxa

    , sum(cp_activity.time_spent)
        filter (where cp_activity.main_specialization = 'Care Coordinator') /60
        as active_time_to_resolve_cc
    , sum(cp_activity.time_spent)
        filter (where cp_activity.main_specialization = 'Nurse Clinician') /60
        as active_time_to_resolve_nc
    , sum(cp_activity.time_spent) /60
        as active_time_to_resolve_total

    -- , sum(cp_activity.time_spent)
    --     filter (where cp_activity.main_specialization = 'Care Coordinator'
    --     	and cp_activity.activity_start < apt_booking.timestamp_est) /60
    --     as active_time_to_apt_booking_cc
    -- , sum(cp_activity.time_spent)
    --     filter (where cp_activity.main_specialization = 'Nurse Clinician'
    --     	and cp_activity.activity_start < apt_booking.timestamp_est) /60
    --     as active_time_to_apt_booking_nc
    -- , sum(cp_activity.time_spent)
    --     filter (where cp_activity.activity_start < apt_booking.timestamp_est) /60
    --     as active_time_to_apt_booking_total

    -- , sum(cp_activity.time_spent)
    --     filter (where cp_activity.main_specialization = 'Care Coordinator'
    --         and cp_activity.activity_start between episodes.dxa_completed_at and apt_booking.timestamp_est) /60
    --     as active_time_to_apt_booking_from_dxa_cc
    -- , sum(cp_activity.time_spent)
    --     filter (where cp_activity.main_specialization = 'Nurse Clinician'
    --         and cp_activity.activity_start between episodes.dxa_completed_at and apt_booking.timestamp_est) /60
    --     as active_time_to_apt_booking_from_dxa_nc
    -- , sum(cp_activity.time_spent)
    --     filter (where cp_activity.activity_start between episodes.dxa_completed_at and apt_booking.timestamp_est) /60
    --     as active_time_to_apt_booking_from_dxa_total

    , sum(cp_activity.time_spent)
        filter (where cp_activity.main_specialization = 'Care Coordinator'
            and cp_activity.activity_start > episodes.dxa_completed_at) /60
        as active_time_to_resolve_from_dxa_cc
    , sum(cp_activity.time_spent)
        filter (where cp_activity.main_specialization = 'Nurse Clinician'
            and cp_activity.activity_start > episodes.dxa_completed_at) /60
        as active_time_to_resolve_from_dxa_nc
    , sum(cp_activity.time_spent)
        filter (where cp_activity.activity_start > episodes.dxa_completed_at) /60
        as active_time_to_resolve_from_dxa_total

from episodes
left join cp_activity
	on episodes.episode_id = cp_activity.episode_id
	and tsrange(
			episodes.first_set_active,
			episodes.first_set_resolved_pending_at
		) @> cp_activity.activity_start
	and cp_activity.is_active
where episodes.first_set_active < episodes.first_set_resolved_pending_at
	and date_trunc('day', episodes.first_message_patient)
		= date_trunc('day', episodes.first_message_patient)
{{ dbt_utils.group_by(13) }}
