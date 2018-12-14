with responses as (
        select * from {{ ref('messaging_practitioner_responses') }}
    )

    , wiw_shifts as (
        select * from {{ ref('wiw_shifts') }}
    )

    , practitioners as (
        select * from {{ ref('practitioners') }}
    )

    , assignments as (
        select * from {{ ref('assigned_time_detailed') }}
    )

    , episodes as (
        select * from {{ ref('episodes') }}
    )

    , reminders as (
        select * from {{ ref('careplatform_reminders_status_updated') }}
    )

    , shifts as (
        select date_trunc('day', wiw_shifts.start_time_est) as date_day
            , wiw_shifts.shift_id
            , wiw_shifts.shift_schedule_est
            , wiw_shifts.user_id
            , practitioners.user_name
            , wiw_shifts.position_name
            , practitioners.main_specialization
            , wiw_shifts.hours
        from wiw_shifts
        inner join practitioners using (user_id)
        where location_name = 'Virtual Care Platform'
            and date_trunc('day', start_time_est) < current_date
    )

    , reminders_by_shift as (
        select shifts.date_day
            , shifts.shift_id
            , count(reminders.*) as reminders_completed_count
        from shifts
        left join reminders
            on shifts.shift_schedule_est @> reminders.timestamp_est
            and shifts.user_id = reminders.user_id
        where reminder_status = 'completed'
        group by 1,2
    )

    , sm_details as (
        select episode_id
            , first_message_shift_manager
            , first_message_patient
            , extract(epoch from
                age(episodes.first_message_shift_manager,
                    episodes.first_message_patient))/60 as sm_frt
        from episodes
        where first_message_shift_manager is not null
    )

    , dxa_details as (
        select episode_id
            , dxa_started_at
            , first_message_patient
            , extract(epoch from
                age(episodes.dxa_started_at,
                    episodes.first_message_patient))/60 as ttdxa
        from episodes
        where dxa_started_at is not null
    )

    , assignments_by_shift as (
        select shifts.date_day
            , shifts.shift_id
            , sum(assignments.first_response_time_min) as frt_sum
            , count(assignments.first_response_time_min) as frt_count
            , sum(assignments.assigned_time_min) as assigned_time_sum
            , count(assignments.assigned_time_min) as assigned_time_count
            , sum(assignments.assigned_time_min)
                filter (where assignments.count_posts > 3)
                as filtered_assigned_time_sum
            , count(assignments.assigned_time_min)
                filter (where assignments.count_posts > 3)
                as filtered_assigned_time_count
            , sum(assignments.rt_sum) as rt_sum
            , sum(assignments.rt_count) as rt_count
            , sum(sm_details.sm_frt) as sm_frt_sum
            , count(sm_details.sm_frt) as sm_frt_count
            , sum(dxa_details.ttdxa) as time_to_dxa_sum
            , count(dxa_details.ttdxa) as time_to_dxa_count
            , count(distinct episodes.episode_id)
                filter (where date_trunc('day', episodes.first_message_patient)
                    = shifts.date_day) as new_episode_count
            , count(episodes.score) filter (where episodes.category = 'promoter') as count_promoters
            , count(episodes.score) filter (where episodes.category = 'detractor') as count_detractors
            , count(episodes.score) filter (where episodes.category is not null) as count_scores
        from shifts
        left join assignments
            on shifts.shift_schedule_est @>
                timezone('America/Montreal', assignments.assigned_at)
            and shifts.user_id = assignments.assigned_user_id
        left join sm_details
            on shifts.shift_schedule_est @>
                sm_details.first_message_patient
            and shifts.shift_schedule_est @>
                sm_details.first_message_shift_manager
            and assignments.episode_id = sm_details.episode_id
        left join dxa_details
            on shifts.shift_schedule_est @>
                dxa_details.first_message_patient
            and shifts.shift_schedule_est @>
                dxa_details.dxa_started_at
            and assignments.episode_id = dxa_details.episode_id
        left join episodes
            on assignments.episode_id = episodes.episode_id
        where assignments.count_posts > 0
            and episodes.outcome <> 'patient_unresponsive'
        group by 1,2
    )

select shifts.date_day
    , shifts.shift_id
    , shifts.user_id
    , shifts.user_name
    , shifts.main_specialization
    , shifts.position_name
    , shifts.hours
    , reminders_by_shift.reminders_completed_count
    , assignments_by_shift.frt_sum
    , assignments_by_shift.frt_count
    , assignments_by_shift.assigned_time_sum
    , assignments_by_shift.assigned_time_count
    , assignments_by_shift.filtered_assigned_time_sum
    , assignments_by_shift.filtered_assigned_time_count
    , assignments_by_shift.rt_sum
    , assignments_by_shift.rt_count
    , assignments_by_shift.time_to_dxa_sum
    , assignments_by_shift.time_to_dxa_count
    , assignments_by_shift.sm_frt_sum
    , assignments_by_shift.sm_frt_count
    , assignments_by_shift.new_episode_count
    , assignments_by_shift.count_promoters
    , assignments_by_shift.count_detractors
    , assignments_by_shift.count_scores
from shifts
inner join assignments_by_shift using (shift_id)
left join reminders_by_shift using (shift_id)
where shifts.date_day < date_trunc('week', current_date)
