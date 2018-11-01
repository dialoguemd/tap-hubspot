with pages as (
        select * from {{ ref( 'careplatform_pages_activity' ) }}
    )

    , videos as (
        select * from {{ ref( 'careplatform_video_activity' ) }}
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
    )

    , fill_patient_id as (
        select user_id
            -- Define days based on `UTC` timezone
            , timezone('America/Montreal',date) as date
            , timezone('America/Montreal',timestamp) as activity_start
            , lead(timezone('America/Montreal',timestamp)) over 
                (partition by user_id, date order by timezone('America/Montreal',timestamp)) as activity_end
            , case
                when timestamp is null or lead(timestamp) over (partition by user_id, date order by timestamp) is null then 0
                else round(extract('epoch' from lead(timestamp) over (partition by user_id, date order by timestamp) - timestamp)) :: integer
              end as time_spent
            , activity
            , patient_id
            , sum(case when patient_id is null then 0 else 1 end) over (partition by date, user_id order by timestamp) as patient_partition
            , episode_id
            , sum(case when episode_id is null then 0 else 1 end) over (partition by date, user_id order by timestamp) as episode_partition
        from make_union
    )

    , all_activity as (
        select user_id
            , date
            , activity_start
            , activity_end
            , time_spent
            , activity
            , case
                when activity <> 'dashboard' then first_value(patient_id) over (partition by user_id, date, patient_partition order by activity_start)
                else null
              end as patient_id
            , case
                when activity <> 'dashboard' then first_value(episode_id) over (partition by user_id, date, episode_partition order by activity_start)
                else null
              end as episode_id
        from fill_patient_id
    )

    select date
        , activity_start
        , activity_end
        , time_spent
        , activity
        , patient_id
        , case when episode_id like '%?%' then split_part(episode_id, '?', 1)
               when episode_id like '%;%' then split_part(episode_id, ';', 1)
               else episode_id end as episode_id
        , split_part(episode_id, ';'::text, 2) like 'childId=%' as is_child
        , case when split_part(episode_id, ';'::text, 2) like 'childId=%' then split_part(split_part(episode_id, ';'::text, 2),'='::text,2) end as child_id
        , all_activity.user_id
        , (activity = 'video' and time_spent < 45*60)
            or (activity = 'chat' and time_spent < 10*60)
            or (activity = 'dashboard' and time_spent < 5*60)
            as is_active
    from all_activity
