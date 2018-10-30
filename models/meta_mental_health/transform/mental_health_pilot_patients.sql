-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with qnaire_answers as (
        select * from {{ ref('mental_health_qnaire_answers') }}
    )

    , wiw_shifts as (
        select * from {{ ref('wiw_shifts') }}
    )

    , careplatform_video_stream_created as (
        select * from {{ ref('careplatform_video_stream_created') }}
    )

    , careplatform_video_stream_ended as (
        select * from {{ ref('careplatform_video_stream_ended') }}
    )

    , episodes as (
        select * from {{ ref('episodes') }}
    )

    , avg_wages_tmp as (
        select users.main_specialization
            , date_trunc('month', shifts.start_date_est) as month
            , sum(cost) / sum(hours) as hourly_cost
        from wiw_shifts as shifts
        left join pdt.users on shifts.user_id = users.user_id
        where location_name = 'Virtual Care Platform'
        group by 1,2
    )

    , avg_wages as (
        select month
            , avg(hourly_cost) filter(where main_specialization = 'Nurse Clinician') as nc_wage
            , avg(hourly_cost) filter(where main_specialization = 'Care Coordinator') as cc_wage
        from avg_wages_tmp
        group by 1
    )

    , videos as (
        select video_start.episode_id
            , video_start.practitioner_id
            , date_trunc('day', timezone('America/Montreal', video_start.created_at)) as date
            , extract(epoch from
                max(timezone('America/Montreal', video_end.ended_at))
                    - min(timezone('America/Montreal', video_start.created_at))
                    ) as video_length
        from careplatform_video_stream_created as video_start
        left join careplatform_video_stream_ended as video_end
            on video_start.episode_id = video_end.episode_id
            and date_trunc('day', timezone('America/Montreal', video_start.created_at))
                = date_trunc('day', timezone('America/Montreal', video_end.ended_at))
        group by 1,2,3
    )

    , priced_videos as (
        -- TODO Replace with an appointment-based rather than actual
        -- video length event to align with compensation strategy
        select episode_id
            , sum(case when video_length < 900 then 45
                else 90 end) filter(where main_specialization = 'Family Physician') as gp_price
            , sum(case when video_length < 900 then 45
                else 90 end) filter(where main_specialization = 'Psychologist') as psy_price
        from videos
        left join pdt.users
            on videos.practitioner_id = users.user_id
        where video_length between 60 and 324000
            and main_specialization in ('Family Physician', 'Psychologist')
        group by 1
    )

    select qnaire_answers.*
        , users.age
        , users.gender
        , users.organization_name
        , episodes.episode_id
        , episodes.first_priority_level
        , episodes.priority_level as current_priority_level
        , episodes.priority_levels_ordered
        , episodes.messages_total
        , episodes.messages_patient
        , episodes.messages_care_team
        , episodes.score as nps_score
        , episodes.category as nps_category
        , episodes.ttr_total
        , episodes.attr_total
        , episodes.attr_nc as active_time_nurse
        , episodes.attr_np as active_time_np
        , episodes.attr_cc as active_time_cc
        , episodes.attr_gp as active_time_gp
        , episodes.attr_psy as active_time_psy
        -- 0.45 is the estimated CP focus multiplier of NCs
        , coalesce(episodes.attr_nc / 60 / 0.45 * avg_wages.nc_wage, 0) as est_nc_cost
        -- 0.6 is the estimated CP focus multiplier of CCs
        , coalesce(episodes.attr_nc / 60 / 0.6 * avg_wages.cc_wage, 0) as est_cc_cost
        , coalesce(priced_videos.gp_price, 0) as gp_cost
        , coalesce(priced_videos.psy_price, 0) as psy_cost
    from qnaire_answers
    inner join episodes
        on qnaire_answers.user_id = episodes.user_id
        and episodes.issue_type = 'psy-pilot'
    left join priced_videos on episodes.episode_id = priced_videos.episode_id
    left join avg_wages on date_trunc('month', episodes.created_at) = avg_wages.month
    left join pdt.users on qnaire_answers.user_id = users.user_id
    where response_rank = 1
