with dxa_replies_multi as (
		select * from {{ ref('dxa_question_replied_multi') }}
	)

select episode_id
	, qnaire_tid

	-- Pivot table with Jinja by fetching column values and making one
	-- variable per column value
	{% set multi_columns =
		dbt_utils.get_column_values(
			ref('dxa_question_replied_multi'),
			'question_name')
	%}

	{% for multi_column in multi_columns %}
	, min(reply)
		filter (where question_name = '{{multi_column}}') as "{{multi_column}}"
	{% endfor %}

from dxa_replies_multi
group by 1,2
