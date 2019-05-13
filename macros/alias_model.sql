{% macro alias_model(aliases, model) %}

with aliases as (
		select *
		from {{target.schema}}.{{aliases}}
	)

	, model as (
		select *
		from {{target.schema}}.{{model}}
	)

select model.*
	, coalesce(
		aliases.user_id
		, model.anonymous_id
	) as user_id
from model
left join aliases
	using (anonymous_id)

{% endmacro %}
