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

	, episodes_appointment_booking as (
		select * from {{ ref('episodes_appointment_booking') }}
	)

	, episodes_intake as (
		select * from {{ ref('episodes_intake') }}
	)

	, episodes_costs as (
		select * from {{ ref('episodes_costs') }}
	)

	, episodes_dispatch_recommendation as (
		select * from {{ ref('episodes_dispatch_recommendation') }}
	)

	, episodes_video_consultations as (
		select * from {{ ref('episodes_video_consultations') }}
	)

	, users as (
		select * from {{ ref('scribe_users') }}
	)

	, cc_confirmed as (
		select * from {{ ref('countdown_cc_confirmed') }}
	)

select
	{{ dbt_utils.star(
		from=ref('messaging_channels'),
		relation_alias='channels')
	}}

	, {{ dbt_utils.star(
		from=ref('episodes_outcomes'),
		except=["episode_id"],
		relation_alias='episodes_outcomes')
	}}

	, episodes_issue_types.issue_type
	, episodes_issue_types.issue_type_set_timestamp

	, (coalesce(episodes_issue_types.issue_type, 'n/a') ||
		'-' || coalesce(episodes_outcomes.outcome, 'n/a'))
		as issue_type_outcome_pair

	, {{ dbt_utils.star(
		from=ref('episodes_priority_levels'),
		except=["episode_id"],
		relation_alias='episodes_priority_levels')
	}}

	, episodes_ratings.rating

	, episodes_subject.episode_subject
	, episodes_subject.episode_subject as patient_id
	, channels.user_id <> episodes_subject.episode_subject
		as is_dependent_consult

	, episodes_chats_summary.date_day_est
	, episodes_chats_summary.date_week_est
	, episodes_chats_summary.date_month_est
	, episodes_chats_summary.first_message_created_at
	, episodes_chats_summary.last_message_created_at
	, episodes_chats_summary.first_message_care_team
	, episodes_chats_summary.last_message_care_team
	, episodes_chats_summary.first_message_patient
	, episodes_chats_summary.last_message_patient
	, episodes_chats_summary.first_message_nurse
	, episodes_chats_summary.first_message_shift_manager
	, episodes_chats_summary.first_message_from_last_cc
	, episodes_chats_summary.first_message_from_last_nc
	, episodes_chats_summary.last_message_from_last_cc
	, episodes_chats_summary.last_message_from_last_nc
	, episodes_chats_summary.messages_total
	, episodes_chats_summary.messages_patient
	, episodes_chats_summary.messages_care_team
	, episodes_chats_summary.messages_length_total
	, episodes_chats_summary.first_set_resolved_pending_at
	, episodes_chats_summary.first_set_active
	, episodes_chats_summary.set_resolved_pending
	, episodes_chats_summary.includes_follow_up
	, episodes_chats_summary.frt_pt_message
	, episodes_chats_summary.frt_active
	, episodes_chats_summary.is_first_message_in_opening_hours

	, episodes_video_consultations.first_video_consultation_started_at
	, episodes_video_consultations.includes_video_consultation
	, episodes_video_consultations.video_consultation_count
	, episodes_video_consultations.video_consultation_length

	{% for spec in ['gp', 'np', 'psy', 'nutr', 'psy_therapist'] %}
	, episodes_video_consultations.includes_video_consultation_{{spec}}
	, episodes_video_consultations.video_consultation_{{spec}}_count
	, episodes_video_consultations.video_consultation_{{spec}}_length
	{% endfor %}

	, episodes_nps.score
	, episodes_nps.nps_score
	, episodes_nps.category
	, episodes_nps.nps_category

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
	, episodes_created_sequence.appointment_preference
	, episodes_created_sequence.dxa_started_at
	, coalesce(episodes_created_sequence.is_dxa_started, false)
		as is_dxa_started
	, episodes_created_sequence.dxa_completed_at
	, coalesce(episodes_created_sequence.is_dxa_completed, false)
		as is_dxa_completed
	, episodes_created_sequence.dxa_completion_time
	, coalesce(episodes_created_sequence.dxa_resume_count, 0)
		as dxa_resume_count
	, coalesce(episodes_created_sequence.dxa_resume_count, 0) > 0
		as is_dxa_resumed
	, episodes_created_sequence.channel_select_started_at
	, episodes_created_sequence.channel_select_completed_at
	, episodes_created_sequence.video_started_at
	, episodes_created_sequence.video_ended_at
	, extract('epoch' from
			episodes_created_sequence.dxa_started_at
			- episodes_chats_summary.first_message_patient
		) / 60
		as time_to_start_dxa_from_first_message
	, extract('epoch' from
			episodes_created_sequence.dxa_completed_at
			- episodes_chats_summary.first_message_patient
		) / 60
		as time_to_complete_dxa_from_first_message

	, episodes_chief_complaint.cc_code
	, episodes_chief_complaint.cc_label_en
	, episodes_chief_complaint.timestamp as dxa_triggered_at
	, episodes_chief_complaint.cc_code_parsed
	, episodes_chief_complaint.cc_code_manual
	, episodes_chief_complaint.cc_code_parsed_timestamp_est
	, episodes_chief_complaint.cc_code_manual_timestamp_est
	, episodes_chief_complaint.dxa_trigger_type

	, coalesce(episodes_reason_for_visit.reason_for_visit, 'N/A')
		as reason_for_visit

	, episodes_appointment_booking.appointment_booking_first_started_at
	, episodes_appointment_booking.includes_appointment_booking

	, users.family_id
	, users.gender
	, users.language
	, extract('year' from
		age(episodes_chats_summary.first_message_created_at,
		users.birthday)) as age

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
		when episodes_created_sequence.dxa_started_at is null
			and episodes_chats_summary.first_message_care_team is null
		then false
		-- initiated by care team
		when least(
				episodes_created_sequence.dxa_started_at
				, episodes_chats_summary.first_message_care_team
			) < episodes_chats_summary.first_message_patient
		then null
		else extract('epoch' from
			least(
				episodes_created_sequence.dxa_started_at
				, episodes_chats_summary.first_message_care_team
			)
			- episodes_chats_summary.first_message_patient
		) / 60.0 < 15
	end as sla_answered_within_15_minutes

	, episodes_intake.intake_completed_at
	, coalesce(episodes_intake.treatment_category, 'N/A') as treatment_category
	, episodes_intake.intake_time_first_patient_message
	, episodes_intake.intake_time_first_set_active
	, coalesce(episodes_intake.triage_outcome, 'N/A') as triage_outcome

	, case
		when episodes_issue_types.issue_type in ('psy', 'psy-pilot')
		then 'PSY'
		when episodes_video_consultations.includes_video_consultation_gp
		then 'GP'
		when episodes_video_consultations.includes_video_consultation_np
		then 'NP'
		when episodes_outcomes.outcome_category = 'Unsuitable episode'
		then 'Unsuitable episode'
		when episodes_outcomes.outcome_category = 'Navigation'
			or episodes_outcomes.outcome = 'referral_without_navigation'
		then 'Out-refered'
		when episodes_chats_summary.first_message_from_last_nc is not null
		then 'NC'
		else 'Unsuitable episode'
	end as episode_type

	, {{ dbt_utils.star(
		from=ref('episodes_costs'),
		except=["episode_id"],
		relation_alias='episodes_costs')
	}}

	, episodes_dispatch_recommendation.dispatch_recommendation
	, episodes_dispatch_recommendation.dispatch_recommendation_timestamp
	, episodes_dispatch_recommendation.dispatch_recommendation_timestamp_est
	, episodes_dispatch_recommendation.episode_id is not null
		as includes_dispatch_recommendation
	, extract('epoch' from
			episodes_dispatch_recommendation.dispatch_recommendation_timestamp_est
			- episodes_chats_summary.first_message_patient
		) / 60
		as dispatch_time_first_patient_message

	, cc_confirmed.cc_code as cc_code_confirmed
	, cc_confirmed.is_cc_confirmed

from channels

-- Jinja loop for repetitive joins
{% for table in [
		"cc_confirmed",
		"episodes_appointment_booking",
		"episodes_chats_summary",
		"episodes_chief_complaint",
		"episodes_costs",
		"episodes_created_sequence",
		"episodes_dispatch_recommendation",
		"episodes_intake",
		"episodes_issue_types",
		"episodes_kpis",
		"episodes_nps",
		"episodes_outcomes",
		"episodes_priority_levels",
		"episodes_ratings",
		"episodes_reason_for_visit",
		"episodes_subject",
		"episodes_video_consultations"
	]
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
