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

    , user_contracts as (
        select * from {{ ref('scribe_user_contract_detailed') }}
    )

    , practitioners as (
        select * from {{ ref('coredata_practitioners') }}
    )

    , priced_episode_days as (
        select * from {{ ref('medops_priced_episodes') }}
    )

    , priced_episodes as (
        select episode_id
            , sum(priced_episode_days.cc_cost) as est_cc_cost
            , sum(priced_episode_days.nc_cost) as est_nc_cost
            , sum(priced_episode_days.np_cost) as est_np_cost
        from priced_episode_days
        group by 1
    )

    , videos as (
        select video_start.episode_id
            , video_start.practitioner_id
            , date_trunc('day', video_start.timestamp_est) as date_day_est
            , extract(epoch from
                max(video_end.timestamp_est)
                    - min(video_start.timestamp_est)
                    ) as video_length
        from careplatform_video_stream_created as video_start
        left join careplatform_video_stream_ended as video_end
            on video_start.episode_id = video_end.episode_id
            and date_trunc('day', video_start.timestamp_est)
                = date_trunc('day', video_end.timestamp_est)
        group by 1,2,3
    )

    , priced_videos as (
        -- TODO Replace with an appointment-based rather than actual
        -- video length event to align with compensation strategy
        select videos.episode_id
            , sum(case when videos.video_length < 900 then 45 else 90 end)
                filter(where practitioners.main_specialization
                    = 'Family Physician') as gp_price
            , sum(case when videos.video_length < 900 then 45 else 90 end)
                filter(where practitioners.main_specialization
                    = 'Psychologist') as psy_price
        from videos
        inner join practitioners
            on videos.practitioner_id = practitioners.user_id
        where videos.video_length between 60 and 324000
            and practitioners.main_specialization in ('Family Physician', 'Psychologist')
        group by 1
    )

    select episodes.episode_id
        , episodes.patient_id
        , qnaire_answers.qnaire_tid
        , qnaire_answers.stress_qnaire_completed_at
        , qnaire_answers.depression_score
        , qnaire_answers.depression_flag
        , qnaire_answers.anxiety_score
        , qnaire_answers.anxiety_flag
        , qnaire_answers.stress_score
        , qnaire_answers.stress_grouping
        , case
            when qnaire_answers.anxiety_flag is true
                or qnaire_answers.depression_flag is true
                then 'Group 3: MH Concern'
            when qnaire_answers.stress_grouping
                in ('No stress', 'Low stress')
                then 'Group 1: No or Low Stress'
            when qnaire_answers.stress_grouping
                in ('Moderate stress', 'High stress')
                then 'Group 2: Moderate or High Stress'
            when episodes.outcome = 'patient_unresponsive'
                then 'Group 5: Patient Drop Off'
            else 'Group 4: No Questionnaire Given'
            end as mh_grouping
        , extract('year' from
            age(episodes.first_message_patient,
                user_contracts.birthday)
            ) as age
        , user_contracts.gender
        , user_contracts.organization_name
        , episodes.first_message_patient as created_at
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
        , priced_episodes.est_cc_cost
        , priced_episodes.est_nc_cost
        , priced_episodes.est_np_cost
        , coalesce(priced_videos.gp_price, 0) as gp_cost
        , coalesce(priced_videos.psy_price, 0) as psy_cost
    from episodes
    left join qnaire_answers
        on episodes.episode_id = qnaire_answers.episode_id
    left join priced_episodes
        on episodes.episode_id = priced_episodes.episode_id
    left join priced_videos
        on episodes.episode_id = priced_videos.episode_id
    left join user_contracts
        on episodes.user_id = user_contracts.user_id
        and episodes.first_message_patient
            <@ user_contracts.during_est
    where (response_rank = 1 or response_rank is null)
        and episodes.issue_type = 'psy-pilot'
