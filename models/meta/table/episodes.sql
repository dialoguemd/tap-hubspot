-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with channels as (
		select * from {{ ref('messaging_channels') }}
	)

	, test_users as (
		select * from {{ ref('scribe_test_users') }}
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

	, episodes_created_sequence as (
		select * from {{ ref('episodes_created_sequence_detailed') }}
	)

	, episodes_chief_complaint as (
		select * from {{ ref('episodes_chief_complaint') }}
	)

	, episodes_reason_for_visit as (
		select * from {{ ref('episodes_reason_for_visit') }}
	)

	, users as (
		select * from {{ ref('scribe_users') }}
	)

select channels.episode_id
	, channels.user_id
	, 'https://zorro.dialogue.co/conversations/' || channels.episode_id as url_zorro
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

	, (coalesce(episodes_issue_types.issue_type, 'n/a') ||
		'-' || coalesce(episodes_outcomes.outcome, 'n/a'))
		as issue_type_outcome_pair

	, episodes_priority_levels.first_priority_level
	, episodes_priority_levels.priority_level
	, episodes_priority_levels.priority_levels_ordered
	, episodes_priority_levels.priority_first_set_timestamp

	, episodes_ratings.rating

	, episodes_subject.episode_subject
	, episodes_subject.episode_subject as patient_id

	, date_trunc('day', episodes_chats_summary.first_message_created_at)
		as date_day_est
	, date_trunc('week', episodes_chats_summary.first_message_created_at)
		as date_week_est
	, episodes_chats_summary.first_message_created_at
	, episodes_chats_summary.last_message_created_at
	, episodes_chats_summary.first_message_care_team
	, episodes_chats_summary.last_message_care_team
	, episodes_chats_summary.first_message_patient
	, episodes_chats_summary.last_message_patient
	, episodes_chats_summary.first_message_nurse
	, episodes_chats_summary.first_message_shift_manager
	, episodes_chats_summary.messages_total
	, episodes_chats_summary.messages_patient
	, episodes_chats_summary.messages_care_team
	, episodes_chats_summary.messages_length_total
	, episodes_chats_summary.first_set_resolved_pending_at
	, episodes_chats_summary.first_set_active
	, episodes_chats_summary.set_resolved_pending
	, episodes_chats_summary.includes_follow_up
	, episodes_chats_summary.includes_video
	, episodes_chats_summary.includes_video_np
	, episodes_chats_summary.includes_video_gp
	, episodes_chats_summary.includes_video_nc
	, episodes_chats_summary.includes_video_cc
	, episodes_chats_summary.includes_video_psy

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

	, episodes_created_sequence.channel_selected
	, episodes_created_sequence.dxa_started_at
	, episodes_created_sequence.dxa_completed_at
	, episodes_created_sequence.channel_select_started_at
	, episodes_created_sequence.channel_select_completed_at
	, episodes_created_sequence.video_started_at
	, episodes_created_sequence.video_ended_at

	, episodes_chief_complaint.cc_code
	, episodes_chief_complaint.timestamp as dxa_triggered_at

	, episodes_reason_for_visit.reason_for_visit

	, users.gender
	, extract('year' from
		age(episodes_chats_summary.first_message_created_at,
		users.birthday)) as age

		-- Calculate First Response Times
	, case
		when episodes_chats_summary.first_message_patient
			< episodes_chats_summary.first_message_care_team
		then extract('epoch' from episodes_chats_summary.first_message_care_team
			- episodes_chats_summary.first_message_patient) / 60.0
		else null
		end as frt_pt_message
	, case
		when episodes_chats_summary.first_set_active
			< episodes_chats_summary.first_message_care_team
		then extract('epoch' from episodes_chats_summary.first_message_care_team
			- episodes_chats_summary.first_set_active) / 60.0
		else null
		end as frt_active
	, case
		when episodes_created_sequence.dxa_completed_at
			< episodes_chats_summary.first_message_care_team
		then extract('epoch' from episodes_chats_summary.first_message_care_team
			- episodes_created_sequence.dxa_completed_at) / 60.0
		else null
		end as frt_dxa

	, case
		-- invalid episodes
		when episodes_outcomes.first_outcome in (
				'episode_duplicate', 'admin', 'test', 'audit'
			)
			or episodes_outcomes.first_outcome is null
			or not episodes_chats_summary.is_first_message_in_opening_hours
		then null
		when episodes_chief_complaint.timestamp_est is null
			and episodes_chats_summary.first_message_care_team is null
		then false
		-- initiated by care team
		when least(
				episodes_chief_complaint.timestamp_est
				, episodes_chats_summary.first_message_care_team
			) < episodes_chats_summary.first_message_patient
		then null
		else extract('epoch' from
			least(
				episodes_chief_complaint.timestamp_est
				, episodes_chats_summary.first_message_care_team
			)
			- episodes_chats_summary.first_message_patient
		) / 60.0 < 15
	end as sla_answered_within_15_minutes

from channels

-- Jinja loop for reptitive joins
{% for table in
		["episodes_outcomes",
		"episodes_issue_types",
		"episodes_priority_levels",
		"episodes_ratings",
		"episodes_subject",
		"episodes_chats_summary",
		"episodes_nps",
		"episodes_kpis",
		"episodes_created_sequence",
		"episodes_chief_complaint",
		"episodes_reason_for_visit"]
	%}

left join {{table}}
	using (episode_id)

{% endfor %}

-- Don't use `using` on these joins because of multiple user_id fields
left join users
	on episodes_subject.episode_subject = users.user_id

left join test_users
	on episodes_subject.episode_subject = test_users.user_id

where test_users.user_id is null
