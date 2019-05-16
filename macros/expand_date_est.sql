-- This macro creates aggregate date fields for a given timestamp and converts
-- it to EST
-- Fields created: day, week, month, quarter and year

{%- macro expand_date_est(ts_table='', ts_field='timestamp') -%}

	{% for timeframe in ['day', 'week', 'month', 'quarter', 'year'] %}

		{%- if not loop.first -%} , {% endif -%}

		date_trunc('{{timeframe}}', timezone('America/Montreal', {{ts_table}}

		{%- if ts_table != '' -%} . {%- endif -%}

		{{ts_field}})) as date_{{timeframe}}_est

	{% endfor %}

{% endmacro %}
