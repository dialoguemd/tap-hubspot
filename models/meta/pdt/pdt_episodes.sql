-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with messaging_channels as (
        select * from {{ ref('messaging_channels') }}
    )

    , test_users as (
        select * from {{ ref('test_users') }}
    )

    , episodes_chats_summary as (
        select * from {{ ref('episodes_chats_summary') }}
    )

    , episodes_outcomes as (
        select * from {{ ref('episodes_outcomes') }}
    )

    , episodes_issue_types as (
        select * from {{ ref('episodes_issue_types') }}
    )

    , episodes_priority_levels as (
        select * from {{ ref('episodes_priority_levels') }}
    )

    , episodes_ratings as (
        select * from {{ ref('episodes_ratings') }}
    )

    , episodes_subject as (
        select * from {{ ref('episodes_subject') }}
    )

    , episodes_kpis as (
        select * from {{ ref('episodes_kpis') }}
    )

    , episodes_nps as (
        select * from {{ ref('episodes_nps') }}
    )

    select channels.channel_id as episode_id
        , channels.user_id
        , 'https://zorro.dialogue.co/conversations/' || channels.channel_id as url_zorro
        , channels.count_messages
        , channels.created_at
        , channels.updated_at
        , channels.deleted_at
        , channels.deleted_at <> '1970-01-01T00:00:00.000Z' as is_deleted
        , channels.last_post_at

        , episodes_outcomes.first_outcome_category
        , episodes_outcomes.first_outcome
        , episodes_outcomes.outcome_category
        , episodes_outcomes.outcome
        , episodes_outcomes.outcomes_ordered
        , episodes_outcomes.outcome_first_set_timestamp

        , episodes_issue_types.issue_type
        , episodes_issue_types.issue_type_set_timestamp

        , episodes_priority_levels.first_priority_level
        , episodes_priority_levels.priority_level
        , episodes_priority_levels.priority_levels_ordered
        , episodes_priority_levels.priority_first_set_timestamp

        , episodes_ratings.rating

        , episodes_subject.episode_subject

        , episodes_chats_summary.first_message_created_at
        , episodes_chats_summary.last_message_created_at
        , episodes_chats_summary.first_message_care_team
        , episodes_chats_summary.last_message_care_team
        , episodes_chats_summary.first_message_patient
        , episodes_chats_summary.last_message_patient
        , episodes_chats_summary.messages_total
        , episodes_chats_summary.messages_patient
        , episodes_chats_summary.messages_care_team
        , episodes_chats_summary.messages_length_total
        , episodes_chats_summary.first_set_resolved_pending_at
        , episodes_chats_summary.set_resolved_pending
        , episodes_chats_summary.includes_follow_up

        , episodes_nps.score
        , episodes_nps.category

        , episodes_kpis.ttr_total
        , episodes_kpis.attr_total
        , episodes_kpis.attr_nc
        , episodes_kpis.attr_np
        , episodes_kpis.attr_nurse
        , episodes_kpis.attr_cc
        , episodes_kpis.attr_gp
        , episodes_kpis.attr_psy
        , episodes_kpis.attr_nutr
        , episodes_kpis.attr_total_day_1
        , episodes_kpis.attr_nc_day_1
        , episodes_kpis.attr_np_day_1
        , episodes_kpis.attr_nurse_day_1
        , episodes_kpis.attr_cc_day_1
        , episodes_kpis.attr_gp_day_1
        , episodes_kpis.attr_psy_day_1
        , episodes_kpis.attr_nutr_day_1
        , episodes_kpis.attr_total_day_7
        , episodes_kpis.attr_nc_day_7
        , episodes_kpis.attr_np_day_7
        , episodes_kpis.attr_nurse_day_7
        , episodes_kpis.attr_cc_day_7
        , episodes_kpis.attr_gp_day_7
        , episodes_kpis.attr_psy_day_7
        , episodes_kpis.attr_nutr_day_7

        , users.organization_name

  from messaging_channels as channels
  left join episodes_outcomes
    on channels.channel_id = episodes_outcomes.episode_id
  left join episodes_issue_types
    on channels.channel_id = episodes_issue_types.episode_id
  left join episodes_priority_levels
    on channels.channel_id = episodes_priority_levels.episode_id
  left join episodes_ratings
    on channels.channel_id = episodes_ratings.episode_id
  left join episodes_subject
    on channels.channel_id = episodes_subject.episode_id
  left join episodes_chats_summary
    on channels.channel_id = episodes_chats_summary.episode_id
  left join episodes_nps
    on channels.channel_id = episodes_nps.episode_id
  left join episodes_kpis
    on channels.channel_id = episodes_kpis.episode_id
  left join test_users
    on channels.user_id = test_users.user_id::text
  left join pdt.users
    on channels.user_id = users.user_id
  where test_users.user_id is null