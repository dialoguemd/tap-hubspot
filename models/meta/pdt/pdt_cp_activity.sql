-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with all_activity as (
        select * from {{ ref( 'careplatform_all_activity' ) }}
		)

  , shifts as (
        select * from {{ ref( 'wiw_shifts' ) }}
    )

  , users as (
        select * from pdt.users
  )

  select all_activity.date
      , all_activity.activity_start
      , all_activity.activity_end
      , all_activity.time_spent
      , all_activity.activity
      , all_activity.patient_id
      , all_activity.episode_id
      , all_activity.is_child
      , all_activity.child_id
      , all_activity.user_id
      , users.main_specialization
      , all_activity.is_active
      , shifts.shift_id
      , shifts.position_name as shift_position
      , shifts.location_name as shift_location
      , shifts.start_time as shift_start
      , shifts.end_time as shift_end
  from all_activity
  left join users
      on all_activity.user_id = users.user_id
  left join shifts 
      on all_activity.user_id = shifts.user_id
      and all_activity.activity_start <@ shifts.shift_schedule
