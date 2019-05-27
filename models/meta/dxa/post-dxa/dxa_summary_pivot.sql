with bool as (
		select * from {{ ref('dxa_bool_pivoted') }}
	)

	, multi as (
		select * from {{ ref('dxa_multi_pivoted') }}
	)

	, free_tmp as (
		select * from {{ ref('dxa_questions_free') }}
	)

	, symptoms as (
		select * from {{ ref('countdown_symptoms_replied') }}
	)

	, completed_qnaire as (
		select * from {{ ref('countdown_qnaire_completed') }}
		where qnaire_name = 'dxa'
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

	, dx as (
		select * from {{ ref('dxa_dx') }}
	)

	, free as (
		select qnaire_tid
			, max(symptoms_duration) as symptoms_duration
			, max(menses_duration) as menses_duration
		from free_tmp
		group by 1
	)

select episodes.triage_outcome
	, case
		when episodes.dispatch_recommendation like 'Outcome WIC%'
		then 'referral'
		when episodes.dispatch_recommendation like 'Outcome Rx%'
		then 'virtual'
		when episodes.dispatch_recommendation in (
			'Outcome Nurse Counselling', 'Outcome NP or MD / OC'
		)
		then 'virtual'
		when episodes.dispatch_recommendation in (
			'Outcome ER', 'Outcome Navigation'
		)
		then episodes.dispatch_recommendation
		when episodes.triage_outcome in ('treated_by_gp', 'treated_by_np')
		then 'virtual'
		when episodes.triage_outcome = 'referral_walk_in'
		then 'referral'
		when episodes.triage_outcome = 'referral_er'
		then 'referral'
		when episodes.triage_outcome = 'navigation'
		then 'referral'
		when episodes.triage_outcome = 'treated_at_ubisoft_clinic'
		then 'referral'
		when episodes.triage_outcome = 'treated_by_nurse'
		then 'virtual'
		else 'N/A'
		end as dispatch_recommendation_simplified
	, case
		when episodes.dispatch_recommendation like 'Outcome WIC%'
		then 'Outcome WIC'
		when episodes.dispatch_recommendation like 'Outcome Rx%'
		then 'Outcome Rx'
		when episodes.dispatch_recommendation is not null
		then episodes.dispatch_recommendation
		when episodes.triage_outcome in ('treated_by_gp', 'treated_by_np')
		then 'Outcome NP or MD / OC'
		when episodes.triage_outcome = 'referral_walk_in'
		then 'Outcome WIC'
		when episodes.triage_outcome = 'referral_er'
		then 'Outcome ER'
		when episodes.triage_outcome = 'navigation'
		then 'Outcome Navigation'
		when episodes.triage_outcome = 'treated_at_ubisoft_clinic'
		then 'Outcome WIC Ubisoft clinic'
		when episodes.triage_outcome = 'treated_by_nurse'
		then 'Outcome Nurse Counselling'
		else 'Other'
		end as dispatch_recommendation_merged
	, episodes.dispatch_recommendation
	, episodes.episode_id
	, episodes.outcome
	, episodes.outcomes_ordered
	, symptoms.descript
	, episodes.cc_code
	, episodes.reason_for_visit
	, episodes.first_message_patient as timestamp
	, episodes.gender
	, episodes.age
	, bool.qnaire_tid
	, completed_qnaire.qnaire_tid is not null as dxa_completed
	, case
		when multi.iqr_doul_endroits in ('abdo', 'headface', 'tho', 'backbutt')
			and multi.iqr_doul_intense :: integer >= 7
			then true
		when bool.flagged_as_dangerous_bool
			then true
		else false
		end as flagged_as_dangerous
	, free.symptoms_duration
	, free.menses_duration

	-- Get pivoted multichoice columns
	, {{ dbt_utils.star(from=ref('dxa_multi_pivoted'),
		except=["episode_id", "qnaire_tid"]) }}

	-- Get pivoted boolean columns
	, {{ dbt_utils.star(from=ref('dxa_bool_pivoted'),
		except=["episode_id", "qnaire_tid", "flagged_as_dangerous_bool"]) }}

	-- Keep only columns of type dx_label_# and dx_score_#
	, {{ dbt_utils.star(from=ref('dxa_dx'),
		except=["qnaire_tid", "cc", "dx_1", "dx_2", "dx_3", "dx_4", "dx_5"]) }}

from bool
inner join episodes
	using (episode_id)
left join symptoms
	on bool.episode_id = symptoms.episode_id
	-- To take the last symptom description
	and symptoms.rank_desc = 1
left join multi
	using (qnaire_tid)
left join free
	using (qnaire_tid)
left join completed_qnaire
	using (qnaire_tid)
left join dx
	using (qnaire_tid)
