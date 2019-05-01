
{{
  config(
    materialized='incremental',
    unique_key='cp_activity_id',
    post_hook=[
       "{{ postgres.index(this, 'cp_activity_id')}}",
    ]
  )
}}

with pages as (
        select * from {{ ref('cp_activity_w_timing') }}
        where activity_start_est < date_trunc('day', current_timestamp)
        {% if is_incremental() %}
            and activity_start_est > (select max(activity_start) from {{ this }})
        {% endif %}
    )

    , shifts as (
        select * from {{ ref('wiw_shifts_detailed') }}
    )

    , subject as (
        select * from {{ ref('episodes_subject') }}
    )

    , issue_type as (
        select * from {{ ref('episodes_issue_types') }}
    )

select pages.cp_activity_id
    , pages.date_day_est as date
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
    and tsrange(pages.activity_start_est, pages.activity_end_est) <@ shifts.shift_schedule_est
left join subject
    using (episode_id)
left join issue_type
    using (episode_id)
