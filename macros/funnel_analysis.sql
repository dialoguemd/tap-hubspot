## Funnel Analysis Macro

## This macro takes an events object and id_for_aggregation field as inputs
## to produce a unioned table for easy funnel visualization in Metabase.
## The events object is comprised of tuples of event_reference and event_rank,
## and an example can be found with `funnel_dob.sql`.

## This version supports filtering by has_preceding_event to exclude nonlinear
## event flows and by time_delta_from_last_event to exclude events that may
## have taken longer to occur than expected.


{% macro funnel_analysis(events, id_for_aggregation) %}

with
	unioned as (

		{% for event in events %}

		select {{id_for_aggregation}}
			, timestamp
			, '{{event}}' as event_name
			, {{loop.index}} as event_rank
		from {{target.schema}}.{{event}}

		{%- if not loop.last %}
			union all
		{% endif -%}

		{% endfor %}

	)

	, linearity as (
		select {{id_for_aggregation}}
			{% for event in events %}
			, bool_or(event_rank = {{loop.index}}) as has_event_rank_{{loop.index}}
			{% endfor %}
		from unioned
		group by 1
	)

select *
	, lag(timestamp)
		over (partition by {{id_for_aggregation}} order by timestamp)
		as previous_timestamp
	, extract(epoch from (timestamp - lag(timestamp)
		over (partition by {{id_for_aggregation}} order by timestamp)))
		as time_delta_from_last_event
	, case 
	{% for event in events %}

		when event_rank = {{loop.index}}

		{% for i in range(1, loop.index) %}
			and has_event_rank_{{i}}
		{% endfor %}

			then true

	{% endfor %}
		else false
		end as has_preceding_events
from unioned
left join linearity
	using ({{id_for_aggregation}})
where {{id_for_aggregation}} is not null

{% endmacro %}
