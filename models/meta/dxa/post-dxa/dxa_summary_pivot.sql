with bool as (
		select * from {{ ref('dxa_bool_pivoted') }}
	)

	, multi as (
		select * from {{ ref('dxa_multi_pivoted') }}
	)

	, free_tmp as (
		select * from {{ ref('dxa_question_replied_free') }}
	)

	, completed_qnaire as (
		select * from {{ ref('countdown_qnaire_completed') }}
		where qnaire = 'dxa'
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
	, case when episodes.triage_outcome in
		('treated_by_gp',
			'treated_by_np',
			'treated_by_nurse') then 'virtual'
		when episodes.triage_outcome in
		('referral_walk_in',
			'referral_er',
			'navigation',
			'treated_at_ubisoft_clinic') then 'referral'
		else 'other'
		end as triage_outcome_simplified
	, episodes.episode_id
	, episodes.outcome
	, episodes.outcomes_ordered
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
	, free.menses_duration,

	-- Get pivoted multichoice columns
	{{ dbt_utils.star(from=ref('dxa_multi_pivoted'),
		except=["episode_id", "qnaire_tid"]) }}

	-- Get pivoted boolean columns
	{{ dbt_utils.star(from=ref('dxa_bool_pivoted'),
		except=["episode_id", "qnaire_tid", "flagged_as_dangerous_bool"]) }}

	{{ dbt_utils.star(from=ref('dxa_dx'),
		except=["qnaire_tid", "cc", "dx_1", "dx_2", "dx_3", "dx_4", "dx_5"]) }}

from bool
inner join episodes
	using (episode_id)
left join multi
	using (qnaire_tid)
left join free
	using (qnaire_tid)
left join completed_qnaire
	using (qnaire_tid)
left join dx
	using (qnaire_tid)
