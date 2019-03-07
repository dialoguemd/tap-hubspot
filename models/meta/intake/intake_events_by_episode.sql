with 
	events as (
		select * from {{ ref( 'intake_events_by_episode_unfiltered' ) }}
	)

    , episodes as (
        select * from {{ ref( 'episodes' ) }}
    )

    , cp_activity as (
        select * from {{ ref( 'cp_activity' ) }}
    )

    , ranks as (
        select episode_id
            , min(rank) filter (where event_name = 'dxa') as dxa_rank
            , min(rank) filter (where event_name = 'channel_selection') as channel_selection_rank
            , min(rank) filter (where type = 'appointment_booking') as apt_booking_rank
            , min(rank) filter (where type = 'resolved') as resolved_rank
            , min(event_name) filter (where rank = '1') as first_rank
        from events
        group by 1
    )

select events.timestamp
    , events.timestamp_est
    , events.during
    , events.event_name
    , events.event_grouping
    , events.user_id
    , events.episode_id
    , events.initiator
    , events.type
    , events.rank
    , events.following_type
    , events.following_event
    , events.duration
    , ranks.dxa_rank
    , ranks.channel_selection_rank
    , ranks.apt_booking_rank
    , ranks.resolved_rank
    , episodes.first_message_care_team
    , episodes.first_message_nurse
    , episodes.dxa_completed_at
    , episodes.cc_code
    , episodes.reason_for_visit
    , episodes.issue_type
    , episodes.outcome
    , sum(cp_activity.time_spent) as time_spent_total
    , sum(cp_activity.time_spent)
        filter (where main_specialization = 'Care Coordinator')
        as time_spent_cc
    , sum(cp_activity.time_spent)
        filter (where main_specialization = 'Nurse Clinician')
        as time_spent_nc
from events
left join ranks
    using (episode_id)
left join episodes
    using (episode_id)
left join cp_activity
    on events.episode_id = cp_activity.episode_id
    and events.during @> cp_activity.activity_start
    and cp_activity.is_active
where (events.rank <= ranks.resolved_rank or ranks.resolved_rank is null)
    -- Only include full episode event sequences i.e. don't include partials
    -- that could start on an event like dxa_permission
    and ranks.first_rank = 'top_level_greeting'
{{ dbt_utils.group_by(24) }}
