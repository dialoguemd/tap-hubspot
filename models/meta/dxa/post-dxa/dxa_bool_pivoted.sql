with dxa_replies_bool as (
		select * from {{ ref('dxa_question_replied_bool') }}
	)

select episode_id
	, qnaire_tid
	, bool_or(flagged_as_dangerous) as flagged_as_dangerous_bool

	-- Pivot table with Jinja by fetching column values and making one
	-- variable per column value
	{% set bool_columns =
		dbt_utils.get_column_values(
			ref('dxa_question_replied_bool'),
			'question_name')
	%}

	{% for bool_column in bool_columns %}
	, min(reply_value)
		filter (where question_name = '{{bool_column}}') as "{{bool_column}}"
	{% endfor %}

from dxa_replies_bool
group by 1,2