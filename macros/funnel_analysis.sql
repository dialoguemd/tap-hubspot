## Funnel Analysis Macro

## This macro takes an events object and id_for_aggregation field as inputs
## to produce a unioned table for easy funnel visualization in Metabase.
## The events object is comprised of tuples of event_reference and event_rank,
## and an example can be found with `funnel_dob.sql`.

## This version supports filtering by has_preceding_event to exclude nonlinear
## event flows and by time_delta_from_last_event to exclude events that may
## have taken longer to occur than expected.

## Note that the standard use case for this macro is with a DAG-style funnel.
## This could work for a non-DAG funnel but it's hacky and you will have to
## always filter on a nonzero time_delta_from_last_event.


{% macro funnel_analysis(events, id_for_aggregation) %}

with
	user_contract as (
		select * from {{ ref('user_contract') }}
	)

	, unioned as (

		{% for event in events %}

		select {{id_for_aggregation}}
			, timestamp
			, {{loop.index}} || '. {{event}}' as event_name
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

select unioned.timestamp
	, unioned.event_name
	, unioned.event_rank
	, linearity.*
	, lag(unioned.timestamp)
		over (partition by unioned.{{id_for_aggregation}} order by unioned.timestamp)
		as previous_timestamp
	, extract(epoch from (unioned.timestamp - lag(unioned.timestamp)
		over (partition by unioned.{{id_for_aggregation}} order by unioned.timestamp)))
		as time_delta_from_last_event
	, case 
	{% for event in events %}

		when unioned.event_rank = {{loop.index}}

		{% for i in range(1, loop.index) %}
			and linearity.has_event_rank_{{i}}
		{% endfor %}

			then true

	{% endfor %}
		else false
		end as has_preceding_events
	{%- if id_for_aggregation == 'user_id' %}
	, user_contract.organization_id
	, user_contract.organization_name
	, user_contract.account_id
	, user_contract.account_name
	, user_contract.gender
	, user_contract.language
	, user_contract.residence_province
	{% endif -%}
from unioned
left join linearity
	using ({{id_for_aggregation}})
{%- if id_for_aggregation == 'user_id' %}
left join user_contract
	on unioned.user_id = user_contract.user_id
	and unioned.timestamp <@ user_contract.during
{% endif -%}
where unioned.{{id_for_aggregation}} is not null

{% endmacro %}
