with dxa_replies_bool as (
		select * from {{ ref('dxa_question_replied_bool') }}
	)

	, dxa_replies_multi as (
		select * from {{ ref('dxa_question_replied_multichoice') }}
	)

	, completed_qnaire as (
		select * from {{ ref('countdown_qnaire_completed') }}
		where qnaire = 'dxa'
	)

	, episodes as (
		select * from {{ ref('episodes') }}
	)

	, pivoted as (
		select episode_id
			, qnaire_tid
			, bool_or(flagged_as_dangerous) as flagged_as_dangerous_bool

			-- Pivot table with Jinja by fetching column values and making one
			-- variable per column value
			{% set columns =
				dbt_utils.get_column_values(
					ref('dxa_question_replied_bool'),
					'question_name')
			%}

			{% for column in columns %}
			, min(reply_value)
				filter (where question_name = '{{column}}') as "{{column}}"
			{% endfor %}

		from dxa_replies_bool
		group by 1,2
	)

	, multichoice as (
		select episode_id
			, qnaire_tid
			, max(pain_intensity) as pain_intensity
			, max(pain_location) as pain_location
			, max(pain_location_detailed) as pain_location_detailed
		from dxa_replies_multi
		group by 1,2
	)

select episodes.triage_outcome
	, episodes.outcome
	, episodes.outcomes_ordered
	, episodes.cc_code
	, episodes.reason_for_visit
	, episodes.first_message_patient as timestamp
	, multichoice.pain_intensity
	, multichoice.pain_location
	, multichoice.pain_location_detailed
	, completed_qnaire.qnaire_tid is not null as dxa_completed
	, case
		when pain_location in ('abdo', 'headface', 'tho', 'backbutt')
			and pain_intensity >= 7
			then true
		when pivoted.flagged_as_dangerous_bool
			then true
		else false
		end as flagged_as_dangerous
	, pivoted.*
from pivoted
inner join episodes
	using (episode_id)
left join multichoice
	using (qnaire_tid)
left join completed_qnaire
	using (qnaire_tid)
