{% macro days_in_range(range) %}

(
	date_trunc('day',UPPER({{range}}))::DATE
	- date_trunc('day',LOWER({{range}}))::DATE
)

{% endmacro %}
