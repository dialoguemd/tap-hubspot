
{{
  config(
    materialized='incremental',
    unique_key='cp_activity_id',
    post_hook=[
       "{{ postgres.index(this, 'cp_activity_id')}}",
    ]
  )
}}

with
    activity as (
        select * from {{ ref('cp_activity_unioned_activity')}}
        where timestamp < date_trunc('day', current_timestamp)
        {% if is_incremental() %}
            and timestamp > (select max(activity_start_est) from {{ this }})
        {% endif %}
    )

    , practitioners as (
        select * from {{ ref('practitioners')}}
    )

    , activity_w_timing_tmp as (
        select activity.cp_activity_id
            , activity.user_id
            , activity.date_day_est
            , activity.timestamp_est as activity_start_est
            , lead(activity.timestamp_est) over
                (partition by activity.user_id, date_day_est order by
                    activity.timestamp_est) as activity_end_est
            , case
                when activity.timestamp_est is null
                    or lead(activity.timestamp_est) over
                    (partition by activity.user_id, activity.date_day_est
                        order by activity.timestamp_est) is null then 0
                else round(extract('epoch' from lead(activity.timestamp_est)
                    over (partition by activity.user_id,
                        activity.date_day_est
                    order by activity.timestamp_est)
                        - activity.timestamp_est)) :: integer
                end as time_spent
            , activity.activity
            , activity.episode_id
            , coalesce(practitioners.main_specialization, 'N/A')
                as main_specialization
        from activity
        left join practitioners
            using (user_id)
    )

    , activity_w_timing as (
        select cp_activity_id
            , user_id
            , date_day_est
            , activity_start_est
            , activity_end_est
            , tsrange(activity_start_est, activity_end_est) as activity_during
            , time_spent
            , activity
            , main_specialization
            , episode_id
        from activity_w_timing_tmp
    )

select * from activity_w_timing
