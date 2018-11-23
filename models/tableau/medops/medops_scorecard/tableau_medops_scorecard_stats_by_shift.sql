with chats as (
        select * from {{ ref('chats_all_time') }}
    )

    , wiw_shifts as (
        select * from {{ ref('wiw_shifts') }}
    )

    , practitioners as (
        select * from {{ ref('coredata_practitioners') }}
    )

    , assignments as (
        select * from {{ ref('assigned_time_detailed') }}
    )

    , episodes as (
        select * from {{ ref('episodes') }}
    )

    , shifts as (
        select date_trunc('day', wiw_shifts.start_time_est) as date_day
            , wiw_shifts.shift_id
            , wiw_shifts.shift_schedule_est
            , wiw_shifts.user_id
            , wiw_shifts.full_name
            , wiw_shifts.position_name
            , practitioners.main_specialization
            , wiw_shifts.hours
        from wiw_shifts
        inner join practitioners using (user_id)
        where location_name = 'Virtual Care Platform'
            and date_trunc('day', start_time_est) < current_date
    )

    , chats_by_shift as (
        select shifts.date_day
            , shifts.shift_id
            , sum(chats.avg_wait_time_following_messages) as rt_sum
            , count(chats.avg_wait_time_following_messages) as rt_count
        from shifts
        left join chats
            on shifts.shift_schedule_est @> chats.first_message_care_team
            and shifts.user_id = chats.first_care_team_user_id
        group by 1,2
    )

    , assignments_by_shift as (
        select shifts.date_day
            , shifts.shift_id
            , sum(assignments.first_response_time_min) as frt_sum
            , count(assignments.first_response_time_min) as frt_count
            , sum(assignments.dispatch_time_min) as dispatch_time_sum
            , count(assignments.dispatch_time_min) as dispatch_time_count
            , sum(assignments.dispatch_time_min)
                filter (where assignments.assignment_type = 'First Assignment')
                as first_dispatch_time_sum
            , count(assignments.dispatch_time_min)
                filter (where assignments.assignment_type = 'First Assignment')
                as first_dispatch_time_count
        from shifts
        left join assignments
            on shifts.shift_schedule_est @>
                timezone('America/Montreal', assignments.assigned_at)
            and shifts.user_id = assignments.assigned_user_id
        left join episodes using (episode_id)
        where assignments.count_posts > 0
            and episodes.outcome <> 'patient_unresponsive'
        group by 1,2
    )

select shifts.date_day
    , shifts.shift_id
    , shifts.user_id
    , shifts.full_name
    , shifts.main_specialization
    , shifts.position_name
    , shifts.hours
    , chats_by_shift.rt_sum
    , chats_by_shift.rt_count
    , assignments_by_shift.frt_sum
    , assignments_by_shift.frt_count
    , assignments_by_shift.dispatch_time_sum
    , assignments_by_shift.dispatch_time_count
    , assignments_by_shift.first_dispatch_time_sum
    , assignments_by_shift.first_dispatch_time_count
from shifts
inner join chats_by_shift using (shift_id)
inner join assignments_by_shift using (shift_id)
where shifts.date_day < date_trunc('week', current_date)
