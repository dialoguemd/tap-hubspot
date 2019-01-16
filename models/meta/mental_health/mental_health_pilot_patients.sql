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

    , episodes as (
        select * from {{ ref('episodes') }}
    )

    , user_contracts as (
        select * from {{ ref('scribe_user_contract_detailed') }}
    )

    , videos_detailed as (
        select * from {{ ref('videos_detailed') }}
    )

    , est_costs_daily as (
        select * from {{ ref('costs_by_episode_daily') }}
    )

    , priced_episodes as (
        select episode_id
            , sum(cc_cost) as est_cc_cost
            , sum(nc_cost) as est_nc_cost
            , sum(np_cost) as est_np_cost
            , sum(gp_psy_cost) as est_gp_psy_cost
            , sum(gp_other_cost) as est_gp_other_cost
        from est_costs_daily
        group by 1
    )

    , videos_per_episode as (
        select episode_id
            , count(video_length)
                filter (where main_specialization = 'Family Physician') as gp_videos_count
            , count(video_length)
                filter (where main_specialization = 'Psychologist') as psy_videos_count
            , avg(video_length) as video_length_avg
            , min(video_length) as video_length_min
            , max(video_length) as video_length_max
            , min(started_at_est) as video_first_started_at
        from videos_detailed
        where video_length > 2
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
        , episodes.last_message_created_at as last_active_at
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
        , priced_episodes.est_gp_other_cost as gp_cost
        , priced_episodes.est_gp_psy_cost as psy_cost
        , videos_per_episode.gp_videos_count
        , videos_per_episode.psy_videos_count
        , videos_per_episode.video_length_avg
        , videos_per_episode.video_length_min
        , videos_per_episode.video_length_max
        , videos_per_episode.video_first_started_at
        , extract(epoch from videos_per_episode.video_first_started_at
            - episodes.first_message_patient)/3600 as time_to_video_hours
    from episodes
    left join qnaire_answers
        using (episode_id)
    left join priced_episodes
        using (episode_id)
    left join videos_per_episode
        using (episode_id)
    left join user_contracts
        on episodes.user_id = user_contracts.user_id
        and episodes.first_message_patient
            <@ user_contracts.during_est
    where (response_rank = 1 or response_rank is null)
        and episodes.issue_type = 'psy-pilot'
