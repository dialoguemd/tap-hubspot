{{ config(materialized='table') }}

with pages as (
        select * from {{ ref('cp_activity_w_timing') }}
    )

    , shifts as (
        select * from {{ ref('wiw_shifts') }}
    )

    , subject as (
        select * from {{ ref('episodes_subject') }}
    )

    , issue_type as (
        select * from {{ ref('episodes_issue_types') }}
    )

select pages.date_day_est as date
    , pages.activity_start_est as activity_start
    , pages.activity_end_est as activity_end
    , pages.time_spent
    , pages.activity
    , subject.episode_subject as patient_id
    , pages.episode_id
    , pages.user_id
    , pages.main_specialization
    , (pages.activity = 'video' and pages.time_spent < 45*60)
        or (pages.activity = 'chat' and pages.time_spent < 10*60)
        or (pages.activity = 'dashboard' and pages.time_spent < 5*60)
        as is_active
    , shifts.shift_id is not null as is_on_shift
    , shifts.shift_id
    , shifts.position_name as shift_position
    , shifts.location_name as shift_location
    , shifts.start_time_est as shift_start
    , shifts.end_time_est as shift_end
    , issue_type.issue_type
from pages
left join shifts
    on pages.user_id = shifts.user_id
    and pages.activity_start_est <@ shifts.shift_schedule_est
left join subject
    using (episode_id)
left join issue_type
    using (episode_id)
