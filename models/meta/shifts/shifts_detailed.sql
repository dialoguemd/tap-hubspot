with shifts as (
        select * from {{ ref('wiw_shifts_detailed') }}
    )

    , practitioners as (
        select * from {{ ref('practitioners') }}
    )

    , shifts_assignments as (
        select * from {{ ref('shifts_assignments') }}
    )

    , shifts_reminders as (
        select * from {{ ref('shifts_reminders') }}
    )

    , shifts_templates as (
        select * from {{ ref('shifts_templates') }}
    )

select shifts.start_date_est as date_day
    , shifts.shift_id
    , shifts.user_id
    , shifts.position_name
    , shifts.hours
    , practitioners.user_name
    , practitioners.main_specialization
    , shifts_assignments.frt_sum
    , shifts_assignments.frt_count
    , shifts_assignments.assigned_time_sum
    , shifts_assignments.assigned_time_count
    , shifts_assignments.filtered_assigned_time_sum
    , shifts_assignments.filtered_assigned_time_count
    , shifts_assignments.rt_sum
    , shifts_assignments.rt_count
    , shifts_assignments.new_episode_count
    , shifts_assignments.count_promoters
    , shifts_assignments.count_detractors
    , shifts_assignments.count_scores
    , shifts_assignments.videos_count
    , shifts_assignments.video_length_sum
    , shifts_assignments.phone_calls_count
    , shifts_assignments.phone_calls_length_sum
    , shifts_reminders.reminders_completed_count
    , shifts_templates.appointments_booked_count
from shifts
inner join practitioners using (user_id)
inner join shifts_assignments using (shift_id)
left join shifts_reminders using (shift_id)
left join shifts_templates using (shift_id)
where shifts.location_name = 'Virtual Care Platform'
