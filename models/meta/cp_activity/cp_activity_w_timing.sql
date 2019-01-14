with
    activity as (
        select * from {{ ref('cp_activity_unioned_activity')}}
    )

    , practitioners as (
        select * from {{ ref('practitioners')}}
    )

    , activity_w_timing_tmp as (
        select activity.user_id
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
        select user_id
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
