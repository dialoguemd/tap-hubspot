with cp_activity as (
    select * from {{ ref( 'cp_activity' ) }}
    )

    select episode_id
        , date_trunc('day', activity_start) as date
        , sum(time_spent) filter(where main_specialization = 'Care Coordinator')*1.0 / 3600 as cc_time
        , sum(time_spent) filter(where main_specialization in ('Nurse Clinician', 'Nurse'))*1.0 / 3600 as nc_time
        , sum(time_spent) filter(where main_specialization = 'Nurse Practitioner')*1.0 / 3600 as np_time
    from cp_activity
    where activity_start > '2018-01-01'
        and shift_location = 'Virtual Care Platform'
        and is_active
        and time_spent > 0
        and episode_id is not null
    group by 1,2
