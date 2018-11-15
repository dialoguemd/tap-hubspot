-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with pages as (
        select * from {{ ref( 'careplatform_pages_activity' ) }}
    )

    , videos as (
        select * from {{ ref( 'video_started' ) }}
    )

    , make_union as (
        select *
        from pages
        /* union
        select *
        from message */
        union
        select *
        from videos
        where careplatform_user_id is not null
    )

    , fill_patient_id as (
        select careplatform_user_id as user_id
            , date_day_est
            , timestamp_est as activity_start
            , lead(timestamp_est) over
                (partition by careplatform_user_id, date_day_est order by
                    timestamp_est) as activity_end
            , case
                when timestamp_est is null or lead(timestamp_est) over 
                    (partition by careplatform_user_id, date_day_est
                        order by timestamp_est) is null then 0
                else round(extract('epoch' from lead(timestamp_est) over
                    (partition by careplatform_user_id, date_day_est
                        order by timestamp_est) - timestamp_est)) :: integer
              end as time_spent
            , activity
            , patient_id
            , sum(case when patient_id is null then 0 else 1 end)
                over (partition by date_day_est, careplatform_user_id
                    order by timestamp_est) as patient_partition
            , episode_id
            , sum(case when episode_id is null then 0 else 1 end)
                over (partition by date_day_est, careplatform_user_id
                    order by timestamp_est) as episode_partition
        from make_union
    )

    , all_activity_tmp as (
        select user_id
            , date_day_est
            , activity_start
            , activity_end
            , time_spent
            , activity
            , case
                when activity <> 'dashboard' then first_value(patient_id)
                    over (partition by user_id, date_day_est, patient_partition order by activity_start)
                else null
              end as patient_id
            , case
                when activity <> 'dashboard' then first_value(episode_id)
                    over (partition by user_id, date_day_est, episode_partition order by activity_start)
                else null
              end as episode_id
        from fill_patient_id
    )

    , all_activity as (
        select date_day_est as date
            , activity_start
            , activity_end
            , time_spent
            , activity
            , patient_id
            , case when episode_id like '%?%' then split_part(episode_id, '?', 1)
                   when episode_id like '%;%' then split_part(episode_id, ';', 1)
                   else episode_id end as episode_id
            , split_part(episode_id, ';'::text, 2) like 'childId=%' as is_child
            , case when split_part(episode_id, ';'::text, 2) like 'childId=%'
                then split_part(split_part(episode_id, ';'::text, 2),'='::text,2) end as child_id
            , user_id
            , (activity = 'video' and time_spent < 45*60)
                or (activity = 'chat' and time_spent < 10*60)
                or (activity = 'dashboard' and time_spent < 5*60)
                as is_active
        from all_activity_tmp
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
    , shifts.start_time_est as shift_start
    , shifts.end_time_est as shift_end
from all_activity
left join users
    on all_activity.user_id = users.user_id
left join shifts
    on all_activity.user_id = shifts.user_id
    and all_activity.activity_start <@ shifts.shift_schedule_est
