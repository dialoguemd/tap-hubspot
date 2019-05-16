-- This macro converts a timestamp to EST
{%- macro to_est(ts_table='', ts_field='timestamp') -%}

	timezone('America/Montreal', {{ts_table}}

	{%- if ts_table != '' -%} . {%- endif -%}

	{{ts_field}}) as {{ts_field}}_est

{%- endmacro %}
