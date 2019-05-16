with
	chats_summary as (
		select * from {{ ref('episodes_chats_summary') }}
	)

	, video_summary as (
		select * from {{ ref('episodes_video_consultations') }}
	)

	, appointment_booking as (
		select * from {{ ref('episodes_appointment_booking') }}
	)

	, outcomes as (
		select * from {{ ref('episodes_outcomes') }}
	)

	, intake as (
		select chats_summary.episode_id
			, chats_summary.first_message_patient
			, chats_summary.first_set_active
			-- Create categorical variable for identifying the correct resource for treatment
			, case
				when chats_summary.first_message_patient
					> chats_summary.first_message_care_team
				then 'other'
				when video_summary.includes_video_consultation_np
					or video_summary.includes_video_consultation_gp
					or appointment_booking.appointment_booking_first_started_at
						is not null
					then 'treated_by_np_gp'
				when outcomes.first_outcome_category = 'Navigation'
					or outcomes.first_outcome = 'referral_without_navigation'
					then 'outreferred'
				when outcomes.first_outcome_category = 'Diagnostic'
					then 'treated_by_nc'
				else 'other'
			end as treatment_category
			, case
				when chats_summary.first_message_patient
					> least(
						chats_summary.first_message_care_team,
						chats_summary.first_set_resolved_pending_at,
						appointment_booking.appointment_booking_first_started_at
					)
					or chats_summary.first_set_active
					> least(
						chats_summary.first_message_care_team,
						chats_summary.first_set_resolved_pending_at,
						appointment_booking.appointment_booking_first_started_at
					)
				then null
				when video_summary.includes_video_consultation_np
					or video_summary.includes_video_consultation_gp
					or appointment_booking.appointment_booking_first_started_at
						is not null
					then least(
						chats_summary.last_message_from_last_nc,
						chats_summary.first_message_from_last_cc,
						appointment_booking.appointment_booking_first_started_at,
						chats_summary.first_set_resolved_pending_at
					)
				when outcomes.first_outcome_category = 'Navigation'
					or outcomes.first_outcome = 'referral_without_navigation'
					then least(
						chats_summary.last_message_from_last_nc,
						chats_summary.first_set_resolved_pending_at
					)
				when outcomes.first_outcome_category = 'Diagnostic'
					then least(
						chats_summary.first_message_from_last_nc,
						chats_summary.first_set_resolved_pending_at
					)
				else null
			end as intake_completed_at
			-- triage groups for post-dxa recommendation training
			, case
				when outcomes.outcome = 'ubisoft'
				then 'treated_at_ubisoft_clinic'
				when video_summary.includes_video_consultation_gp
				then 'treated_by_gp'
				when video_summary.includes_video_consultation_np
				then 'treated_by_np'
				when chats_summary.first_message_nurse is not null
					and outcomes.outcome_category = 'Diagnostic'
				then 'treated_by_nurse'
				-- Split into locations for referral
				when 'er' = ANY(outcomes.outcomes_ordered)
				then 'referral_er'
				when 'walkin_clinic' = ANY(outcomes.outcomes_ordered)
					or 'referral_without_navigation' = ANY(outcomes.outcomes_ordered)
				then 'referral_walk_in'
				when 'navigation_only' = ANY(outcomes.outcomes_ordered)
				then 'navigation'
				else 'other'
		    end as triage_outcome
		from chats_summary
		left join appointment_booking
			using (episode_id)
		left join video_summary
			using (episode_id)
		left join outcomes
			using (episode_id)
	)

select *
	-- Calculate intake time from first patient message to when the correct resource
	-- was determined and next step (e.g. apt booking, navigation) was started
	, extract(epoch from intake_completed_at - first_message_patient) / 60
		as intake_time_first_patient_message
	-- Calculate intake time from first set active to when the correct resource
	-- was determined and next step (e.g. apt booking, navigation) was started
	, extract(epoch from intake_completed_at - first_set_active) / 60
		as intake_time_first_set_active
from intake
